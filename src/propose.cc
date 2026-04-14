#include "cas_paxos_st.h"
#include "util.h"

using namespace paxos_st;

void CasPaxos::Propose([[maybe_unused]] uint32_t len,
                       [[maybe_unused]] uint8_t* buf) {
  Value v;
#ifndef STANDALONE
  v.SetId(host_id_);
  v.SetOffset(buf_offset_);
#endif
  v = *reinterpret_cast<uint32_t*>(buf);
  ProposeInternal(v);
}

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
void CasPaxos::ProposeInternal(Value& v) {
  auto backoff = std::chrono::nanoseconds(std::rand() % kMaxStartingBackoff);
  bool done = false;
  State* curr_proposal;

#ifdef VERBOSE
  ROMULUS_INFO("<ProposeInternal> START value=({}, {}), log_offset={}", v.id(),
               v.offset(), log_offset_);
#endif

  while (!done) {
    bool ok = false;
    ROMULUS_COUNTER_INC("attempts");

#ifdef VERBOSE
    ROMULUS_INFO("<ProposeInternal> Beginning attempt, log_offset={}",
                 log_offset_);
#endif

    if (multi_paxos_opt_) {
      if (!stable_leader_) {
#ifdef VERBOSE
        ROMULUS_INFO(
            "<ProposeInternal> Unstable leader, new_leader_id_={}, host_id_={}",
            new_leader_id_.load(), host_id_);
#endif
        if (new_leader_id_ == host_id_) {
          // Leader election - need to run prepare
#ifdef VERBOSE
          ROMULUS_INFO("<ProposeInternal> Running prepare for leader election");
#endif

          curr_proposal = Prepare();


#ifdef VERBOSE
          ROMULUS_INFO("<ProposeInternal> Prepare returned curr_proposal={}",
                       curr_proposal ? curr_proposal->ToString() : "nullptr");
#endif

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

          local_ballot_ =
              MakeBallot(curr_proposal->GetPromiseBallot() / system_size_ + 1);
#ifdef VERBOSE
          ROMULUS_INFO("<ProposeInternal> Computed local_ballot_={}",
                       local_ballot_);
#endif
          ROMULUS_ASSERT(local_ballot_ % system_size_ == new_leader_id_,
                         "Ballot assignment logic error.");
          State new_leader_state(local_ballot_, 0, Value(kNullValue));
#ifdef VERBOSE
          ROMULUS_INFO("<ProposeInternal> Broadcasting leader state");
#endif
          BroadcastLeader(&new_leader_state);

          uint64_t leader_id = ExtractId(*curr_proposal);
          ROMULUS_INFO("Elected leader {}", leader_id);

          is_leader_ = true;
#ifdef VERBOSE
          ROMULUS_INFO("<ProposeInternal> Set is_leader_=true");
#endif
        }

        stable_leader_ = true;
#ifdef VERBOSE
        ROMULUS_INFO("<ProposeInternal> Set stable_leader_=true");
#endif

        // If we lost election, exit and let test loop handle it
        if (!is_leader_) {
#ifdef VERBOSE
          ROMULUS_INFO("<ProposeInternal> Lost election, returning");
#endif
          return;
        }
      } else {
        // Multi-paxos optimization on, and we are **stable**
#ifdef VERBOSE
        ROMULUS_INFO("<ProposeInternal> Stable leader path, log_offset={}",
                     log_offset_);
#endif
        curr_proposal = &proposed_state_[log_offset_];
        curr_proposal->SetProposal(local_ballot_, v);
#ifdef VERBOSE
        ROMULUS_INFO(
            "<ProposeInternal> Set proposal: ballot={}, value=({}, {})",
            local_ballot_, v.id(), v.offset());
#endif
      }
    } else {
      // Non-Multi-Paxos: prepare every round
#ifdef VERBOSE
      ROMULUS_INFO("<ProposeInternal> Non-Multi-Paxos path, calling Prepare");
#endif
      curr_proposal = Prepare();
#ifdef VERBOSE
      ROMULUS_INFO("<ProposeInternal> Prepare returned curr_proposal={}",
                   curr_proposal ? curr_proposal->ToString() : "nullptr");
#endif
      ROMULUS_DEBUG("Node {} completed prepare with curr_proposal={}", host_id_,
                    curr_proposal->ToString());
      if (curr_proposal != nullptr) {
        uint64_t leader_id = ExtractId(*curr_proposal);
        ROMULUS_INFO("Elected leader {}", leader_id);
        is_leader_ = (leader_id == host_id_);
#ifdef VERBOSE
        ROMULUS_INFO("<ProposeInternal> is_leader_={}", is_leader_);
#endif
      }
    }

    // At this point, we should be the leader (or non-multipaxos with
    // curr_proposal)
#ifdef VERBOSE
    ROMULUS_INFO(
        "<ProposeInternal> Checking if should enter promise: "
        "multi_paxos_opt_={}, is_leader_={}, curr_proposal={}",
        multi_paxos_opt_, is_leader_, curr_proposal ? "valid" : "nullptr");
#endif
    if ((multi_paxos_opt_ && is_leader_) ||
        (!multi_paxos_opt_ && curr_proposal)) {
      // ROMULUS_INFO("Entering promise phase...");
#ifdef VERBOSE
      ROMULUS_INFO("<ProposeInternal> Calling Promise for value=({}, {})",
                   v.id(), v.offset());
#endif
      ok = Promise(v);
#ifdef VERBOSE
      ROMULUS_INFO("<ProposeInternal> Promise returned ok={}", ok);
#endif
      if (ok) {
        auto committed = log_[log_offset_].GetValue();
#ifdef VERBOSE
        ROMULUS_INFO(
            "<ProposeInternal> Committed value=({}, {}), proposed value=({}, "
            "{})",
            committed.id(), committed.offset(), v.id(), v.offset());
#endif
        if (committed == v) {
          ROMULUS_DEBUG(
              "Proposed slot committed: value=({}, {}), log_offset={}", v.id(),
              v.offset(), log_offset_);
          is_leader_ = true;
          done = true;
#ifdef VERBOSE
          ROMULUS_INFO(
              "<ProposeInternal> Proposal succeeded, done=true, incrementing "
              "log_offset from {}",
              log_offset_);
#endif
        } else {
          ROMULUS_DEBUG("Slot committed: value=({}, {}), log_offset={}",
                        committed.id(), committed.offset(), log_offset_);
          is_leader_ = false;
#ifdef VERBOSE
          ROMULUS_INFO(
              "<ProposeInternal> Different value committed, lost leadership");
#endif
        }
        ++log_offset_;
#ifdef VERBOSE
        ROMULUS_INFO("<ProposeInternal> log_offset incremented to {}",
                     log_offset_);
#endif
      } else {
        ROMULUS_COUNTER_INC("p2_aborts");
#ifdef VERBOSE
        ROMULUS_INFO("<ProposeInternal> Promise failed (p2_abort)");
#endif
      }
    } else {
      ROMULUS_COUNTER_INC("p1_aborts");
#ifdef VERBOSE
      ROMULUS_INFO("<ProposeInternal> Skipped promise phase (p1_abort)");
#endif
    }

    // If aborted, backoff and retry
    if (!ok) {
#ifdef VERBOSE
      ROMULUS_INFO("<ProposeInternal> Proposal aborted, ok=false, backing off");
#endif
      is_leader_ = false;
      if (multi_paxos_opt_ && stable_leader_) {
        // We lost leadership
#ifdef VERBOSE
        ROMULUS_INFO(
            "<ProposeInternal> Lost leadership in Multi-Paxos stable mode, "
            "returning");
#endif
        return;
      }
      if (!multi_paxos_opt_) {
        stable_leader_ = false;
#ifdef VERBOSE
        ROMULUS_INFO("<ProposeInternal> Set stable_leader_=false");
#endif
      }
#ifdef VERBOSE
      ROMULUS_INFO("<ProposeInternal> Executing backoff");
#endif
      backoff = DoBackoff(backoff);
#ifdef VERBOSE
      ROMULUS_INFO("<ProposeInternal> Backoff complete, new backoff={} ns",
                   backoff.count());
#endif
    }
  }
#ifdef VERBOSE
  ROMULUS_INFO("<ProposeInternal> Exiting loop, incrementing proposed counter");
#endif
  ROMULUS_COUNTER_INC("proposed");
#ifdef VERBOSE
  ROMULUS_INFO("<ProposeInternal> END value=({}, {}), log_offset={}", v.id(),
               v.offset(), log_offset_);
#endif
}