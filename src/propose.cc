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

  while (!done) {
    bool ok = false;
    ROMULUS_COUNTER_INC("attempts");

    if (multi_paxos_opt_) {
      if (!stable_leader_) {
        if (new_leader_id_ == host_id_) {
          // Leader election - need to run prepare
          curr_proposal = Prepare();
          local_ballot_ =
              MakeBallot(curr_proposal->GetPromiseBallot() / system_size_ + 1);
          ROMULUS_ASSERT(local_ballot_ % system_size_ == new_leader_id_,
                         "Ballot assignment logic error.");
          State new_leader_state(local_ballot_, 0, Value(kNullValue));
          BroadcastLeader(&new_leader_state);

          is_leader_ = true;
        }

        stable_leader_ = true;

        // If we lost election, exit and let test loop handle it
        if (!is_leader_) {
          return;
        }
      } else {
        // Multi-paxos optimization on, and we are **stable**
        curr_proposal = &proposed_state_[log_offset_];
        curr_proposal->SetProposal(local_ballot_, v);
      }
    } else {
      // Non-Multi-Paxos: prepare every round
      curr_proposal = Prepare();
      ROMULUS_DEBUG("Node {} completed prepare with curr_proposal={}", host_id_,
                    curr_proposal->ToString());
      if (curr_proposal != nullptr) {
        Ballot winning_ballot = curr_proposal->GetPromiseBallot();
        uint32_t leader_id = winning_ballot % system_size_;
        is_leader_ = (leader_id == host_id_);
      }
    }

    // At this point, we should be the leader (or non-multipaxos with
    // curr_proposal)
    if ((multi_paxos_opt_ && is_leader_) ||
        (!multi_paxos_opt_ && curr_proposal)) {
      ROMULUS_DEBUG("Entering promise phase...");
      ok = Promise(v);
      if (ok) {
        auto committed = log_[log_offset_].GetValue();
        if (committed == v) {
          ROMULUS_DEBUG(
              "Proposed slot committed: value=({}, {}), log_offset={}", v.id(),
              v.offset(), log_offset_);
          is_leader_ = true;
          done = true;

          if (failover_detected_) {
            auto now = std::chrono::steady_clock::now();
            failover_time_ =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    now - failover_start_time_)
                    .count();
            ROMULUS_INFO("Failover detected! Duration: {} us", failover_time_);
            failover_detected_ = false;
          }

        } else {
          ROMULUS_DEBUG("Slot committed: value=({}, {}), log_offset={}",
                        committed.id(), committed.offset(), log_offset_);
          is_leader_ = false;
        }
        ++log_offset_;
      } else {
        ROMULUS_COUNTER_INC("p2_aborts");
      }
    } else {
      ROMULUS_COUNTER_INC("p1_aborts");
    }

    // If aborted, backoff and retry
    if (!ok) {
      is_leader_ = false;
      if (multi_paxos_opt_ && stable_leader_) {
        // We lost leadership
        return;
      }
      if (!multi_paxos_opt_) {
        stable_leader_ = false;
      }
      backoff = DoBackoff(backoff);
    }
  }
  ROMULUS_COUNTER_INC("proposed");
}