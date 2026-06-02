#include "cas_paxos_st.h"
#include "util.h"

using namespace paxos_st;

void CasPaxos::Propose(uint32_t len, uint8_t* buf) {
  ROMULUS_ASSERT(len > 0 && buf != nullptr, "Invalid proposal.");
  Value v = *reinterpret_cast<uint32_t*>(buf);
  ProposeInternal(v);
}

void CasPaxos::Preprepare() {
  if (multi_paxos_opt_ && host_id_ == new_leader_id_) {
    preprepare_sem_.release();
  }
}

void CasPaxos::Preparer() {
  pin_thread_to_core(8);

  while (threads_running_.load()) {
    preprepare_sem_.acquire();

    while (prep_offset_.load() < capacity_ && host_id_ == new_leader_id_ && threads_running_.load()) {
      Prepare();
    }
  }
}

void CasPaxos::ProposeInternal(Value& v) {
  if (host_id_ != new_leader_id_) {
    ROMULUS_FATAL(
        "Called ProposeInternal on non-leader node. new_leader_id_={}, "
        "host_id_={}",
        new_leader_id_.load(), host_id_);
  }
  if (multi_paxos_opt_) {
    // ###### MULTI-PAXOS PATH ######
    if (!stable_leader_) {
      // Not stable, need to run prepare to elect leader
      ROMULUS_ASSERT(
          Prepare(),
          "ProposeInternal: Prepare failed in unstable leader path.");
      is_leader_ = true;
      stable_leader_ = true;
      // trigger prepreparation
      // preprepare_sem_.release();
    }
    // Stable leader, can skip prepare and go straight to promise
    ROMULUS_ASSERT(Promise(v),
                   "ProposeInternal: Promise failed in stable leader path.");
  } else {
    // ###### NON-MULTI-PAXOS PATH ######
    ROMULUS_ASSERT(Prepare(),
                   "ProposeInternal: Prepare failed in non-multi-paxos path.");
    ROMULUS_ASSERT(Promise(v),
                   "ProposeInternal: Promise failed in non-multi-paxos path.");
  }
}

#if 0
// Repeated attempt to propose the given value until it is successfully
// committed. Initially, try to update the log by repeatedly calling
// TryCatchUp until it returns false, which indicates that the log offset is
// at an entry that was not committed yet. Then, update the ballot to be the
// next highest unique ballot number for that slot. Next, prepare peers by
// attempting to CAS in the newly chosen ballot. Finally, commit the value
// by writing to a quorum of nodes. During this process it is possible that
// a previously proposed value is adopted, in which case the cycle will
// repeat in an attempt to commit the provided value. If the prepare phase
// is successful, then the node considers itself the leader. If it remains
// the leader then future ballot updates and prepare phases will be skipped.
void CasPaxos::ProposeInternal_OLD(Value& v) {
  auto backoff = std::chrono::nanoseconds(std::rand() % kMaxStartingBackoff);
  bool done = false;
  State* curr_proposal;

  ROMULUS_VERBOSE("<ProposeInternal> START value=({}, {}), log_offset={}",
                  v.id(), v.offset(), log_offset_);

  while (!done) {
    bool ok = false;
    ROMULUS_COUNTER_INC("attempts");

    ROMULUS_VERBOSE("<ProposeInternal> Beginning attempt, log_offset={}",
                    log_offset_);

    if (multi_paxos_opt_) {
      if (!stable_leader_) {
        ROMULUS_VERBOSE(
            "<ProposeInternal> Unstable leader, new_leader_id_={}, host_id_={}",
            new_leader_id_.load(), host_id_);
        if (new_leader_id_ == host_id_) {
          // Leader election - need to run prepare
          ROMULUS_VERBOSE(
              "<ProposeInternal> Running prepare for leader election");
          auto prepare_start = std::chrono::high_resolution_clock::now();
          curr_proposal = Prepare();
          auto prepare_end = std::chrono::high_resolution_clock::now();
          ROMULUS_INFO("<ProposeInternal> Prepare phase took {} us",
                       std::chrono::duration_cast<std::chrono::microseconds>(
                           prepare_end - prepare_start)
                           .count());

          ROMULUS_VERBOSE(
              "<ProposeInternal> Prepare returned curr_proposal={}",
              curr_proposal ? curr_proposal->ToString() : "nullptr");
#ifdef FAILOVER
          if (failover_detected_) {
            auto now = std::chrono::steady_clock::now();
            ROMULUS_ASSERT(
                failover_start_time_ != std::chrono::steady_clock::time_point(),
                "Failover detected but failover_start_time_ is not set");
            failover_time_ =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    now - failover_start_time_)
                    .count();
            ROMULUS_INFO("[FAILOVER TIME] {} us", failover_time_);
            failover_detected_ = false;
          }
#endif

          local_ballot_ =
              MakeBallot(curr_proposal->GetPromiseBallot() / system_size_ + 1);
          ROMULUS_VERBOSE("<ProposeInternal> Computed local_ballot_={}",
                          local_ballot_);
          ROMULUS_ASSERT(local_ballot_ % system_size_ == new_leader_id_,
                         "Ballot assignment logic error.");
          State new_leader_state(local_ballot_, 0, Value(kNullValue));

          ROMULUS_VERBOSE("<ProposeInternal> Broadcasting leader state");

          BroadcastLeader(&new_leader_state);

          uint64_t leader_id = ExtractId(*curr_proposal);
          ROMULUS_INFO("Elected leader {}", leader_id);

          is_leader_ = true;

          ROMULUS_VERBOSE("<ProposeInternal> Set is_leader_=true");
        }

        stable_leader_ = true;

        ROMULUS_VERBOSE("<ProposeInternal> Set stable_leader_=true");

        // If we lost election, exit and let test loop handle it
        if (!is_leader_) {
          ROMULUS_VERBOSE("<ProposeInternal> Lost election, returning");

          return;
        }
      } else {
        // Multi-paxos optimization on, and we are **stable**

        ROMULUS_VERBOSE("<ProposeInternal> Stable leader path, log_offset={}",
                        log_offset_);

        curr_proposal = &proposed_state_[log_offset_];
        curr_proposal->SetProposal(local_ballot_, v);

        ROMULUS_VERBOSE(
            "<ProposeInternal> Set proposal: ballot={}, value=({}, {})",
            local_ballot_, v.id(), v.offset());
      }
    } else {
      // Non-Multi-Paxos: prepare every round

      ROMULUS_VERBOSE(
          "<ProposeInternal> Non-Multi-Paxos path, calling Prepare");

      curr_proposal = Prepare();

      ROMULUS_VERBOSE("<ProposeInternal> Prepare returned curr_proposal={}",
                      curr_proposal ? curr_proposal->ToString() : "nullptr");

      ROMULUS_DEBUG("Node {} completed prepare with curr_proposal={}", host_id_,
                    curr_proposal->ToString());
      if (curr_proposal != nullptr) {
        uint64_t leader_id = ExtractId(*curr_proposal);
        ROMULUS_INFO("Elected leader {}", leader_id);
        is_leader_ = (leader_id == host_id_);

        ROMULUS_VERBOSE("<ProposeInternal> is_leader_={}", is_leader_);
      }
    }

    // At this point, we should be the leader (or non-multipaxos with
    // curr_proposal)

    ROMULUS_VERBOSE(
        "<ProposeInternal> Checking if should enter promise: "
        "multi_paxos_opt_={}, is_leader_={}, curr_proposal={}",
        multi_paxos_opt_, is_leader_, curr_proposal ? "valid" : "nullptr");

    if ((multi_paxos_opt_ && is_leader_) ||
        (!multi_paxos_opt_ && curr_proposal)) {
      // ROMULUS_INFO("Entering promise phase...");

      ROMULUS_VERBOSE("<ProposeInternal> Calling Promise for value=({}, {})",
                      v.id(), v.offset());

      auto promise_start = std::chrono::high_resolution_clock::now();
      ok = Promise(v);
      auto promise_end = std::chrono::high_resolution_clock::now();
      ROMULUS_INFO("<ProposeInternal> Promise phase took {} us",
                   std::chrono::duration_cast<std::chrono::microseconds>(
                       promise_end - promise_start)
                       .count());

      ROMULUS_VERBOSE("<ProposeInternal> Promise returned ok={}", ok);

      if (ok) {
        auto committed = log_[log_offset_].GetValue();

        ROMULUS_VERBOSE(
            "<ProposeInternal> Committed value=({}, {}), proposed value=({}, "
            "{})",
            committed.id(), committed.offset(), v.id(), v.offset());

        if (committed == v) {
          ROMULUS_DEBUG(
              "Proposed slot committed: value=({}, {}), log_offset={}", v.id(),
              v.offset(), log_offset_);
          is_leader_ = true;
          done = true;

          ROMULUS_VERBOSE(
              "<ProposeInternal> Proposal succeeded, done=true, incrementing "
              "log_offset from {}",
              log_offset_);

        } else {
          ROMULUS_DEBUG("Slot committed: value=({}, {}), log_offset={}",
                        committed.id(), committed.offset(), log_offset_);
          is_leader_ = false;

          ROMULUS_VERBOSE(
              "<ProposeInternal> Different value committed, lost leadership");
        }
        ++log_offset_;

        ROMULUS_VERBOSE("<ProposeInternal> log_offset incremented to {}",
                        log_offset_);

      } else {
        ROMULUS_COUNTER_INC("p2_aborts");

        ROMULUS_VERBOSE("<ProposeInternal> Promise failed (p2_abort)");
      }
    } else {
      ROMULUS_COUNTER_INC("p1_aborts");

      ROMULUS_VERBOSE("<ProposeInternal> Skipped promise phase (p1_abort)");
    }

    // If aborted, backoff and retry
    if (!ok) {
      ROMULUS_VERBOSE(
          "<ProposeInternal> Proposal aborted, ok=false, backing off");

      is_leader_ = false;
      if (multi_paxos_opt_ && stable_leader_) {
        // We lost leadership

        ROMULUS_VERBOSE(
            "<ProposeInternal> Lost leadership in Multi-Paxos stable mode, "
            "returning");

        return;
      }
      if (!multi_paxos_opt_) {
        stable_leader_ = false;

        ROMULUS_VERBOSE("<ProposeInternal> Set stable_leader_=false");
      }

      ROMULUS_VERBOSE("<ProposeInternal> Executing backoff");

      backoff = DoBackoff(backoff);
      ROMULUS_VERBOSE("<ProposeInternal> Backoff complete, new backoff={} ns",
                      backoff.count());
    }
  }

  ROMULUS_VERBOSE(
      "<ProposeInternal> Exiting loop, incrementing proposed counter");

  ROMULUS_COUNTER_INC("proposed");

  ROMULUS_VERBOSE("<ProposeInternal> END value=({}, {}), log_offset={}", v.id(),
                  v.offset(), log_offset_);
}
#endif