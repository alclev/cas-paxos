#include "cas_paxos_st.h"
#include "util.h"

using namespace paxos_st;

State* CasPaxos::Prepare() {
  State* curr_proposal = &proposed_state_[log_offset_];
  Ballot curr_promise_ballot = curr_proposal->GetPromiseBallot();
  if (local_ballot_ == 0) {
    local_ballot_ = MakeBallot(1);
  }
  curr_promise_ballot = std::max(curr_promise_ballot, local_ballot_);
  curr_proposal->SetPromiseBallot(curr_promise_ballot);

  auto backoff = std::chrono::nanoseconds(std::rand() % kMaxStartingBackoff);

  std::vector<State> swap(system_size_);
  std::vector<State> state(system_size_);
  uint32_t done_count = 0;

  // Init
  for (uint32_t i = 0; i < system_size_; ++i) {
    expected_[i] = State();
    done_[i] = false;
    swap[i] = State(curr_promise_ballot, 0, Value(0));
    state[i] = State();
  }
  uint64_t wr_id_base = (static_cast<uint64_t>(wr_id_) << 48) |
                        (static_cast<uint64_t>(host_id_) << 32);
  uint32_t cached_offset = log_offset_ * kSlotSize;
  while (done_count < quorum_) {
    ++wr_id_;
    // Post CAS ops.
    int posted = 0;
    for (uint32_t i = 0; i < system_size_; ++i) {
      if (done_[i]) continue;
      if (detected_[i]) {
        done_[i] = true;
        ++done_count;
        continue;
      }
      
      uint64_t wr_id = wr_id_base | static_cast<uint64_t>(i);

      auto& conn = cached_conns_[i];
      auto& raddr = cached_raddrs_[i];
      raddr.addr_info.offset = cached_offset;
      auto& laddr = cached_laddr_;
      laddr.offset = i * kSlotSize;
      
      conn->CompareAndSwap(laddr, raddr, expected_[i].raw, swap[i].raw, wr_id);
      ++posted;
    }
    remote_conns_[0][0]->ProcessCompletions(posted);

    // Check return value of CAS.
    bool need_bump = false;
    Ballot observed_max_ballot = curr_promise_ballot;
    uint32_t winning_index = 0;

    for (uint32_t i = 0; i < system_size_; ++i) {
      if (done_[i]) continue;
      State observed = *contexts_[i]->scratch_state;

      if (observed.raw == expected_[i].raw) {
        ROMULUS_DEBUG("Prepare: cas success");
        state[i] = observed;
        done_[i] = true;
        ++done_count;
      } else {
        ROMULUS_DEBUG(
            "Prepare: cas failed but promise ballot still good. observed={}, "
            "expected={}",
            observed.ToString(), expected_[i].ToString());
        expected_[i] = observed;
        swap[i] = State(curr_promise_ballot, observed.GetBallot(),
                        observed.GetValue());
        state[i] = observed;
        if (observed.GetPromiseBallot() > curr_promise_ballot) {
          need_bump = true;
          observed_max_ballot =
              std::max(observed_max_ballot, observed.GetPromiseBallot());
          winning_index = i;
        }
      }
    }
    // Handle ballot bump after processing all completions
    if (need_bump) {
      if (multi_paxos_opt_) {
        // In multi-paxos we give up -- let them be the leader
        ROMULUS_DEBUG(
            "Prepare: detected higher ballot, yielding to other leader");
        is_leader_ = false;
        // failure condition
        proposed_state_[log_offset_] = state[winning_index];
        return &proposed_state_[log_offset_];
      }
      ROMULUS_DEBUG("Prepare: cas failed. abort and bump.");
      Ballot unique_ballot = BumpBallot(observed_max_ballot);
      ROMULUS_ASSERT(unique_ballot > observed_max_ballot,
                     "GlobalBallot did not exceed observed promise ballot");

      curr_promise_ballot = unique_ballot;
      curr_proposal->SetPromiseBallot(unique_ballot);

      done_count = 0;
      std::fill(done_.begin(), done_.end(), false);

      for (uint32_t j = 0; j < system_size_; ++j) {
        expected_[j] = State();
        swap[j] = State(curr_promise_ballot, 0, Value(0));

        backoff = DoBackoff(backoff);
      }
    }
  }
  // Reduce over the quorum: adopt highest accepted proposal
  Ballot best_ballot = 0;
  Value best_value = Value(0);

  for (uint32_t i = 0; i < system_size_; ++i) {
    if (!done_[i]) continue;
    // we reduce over the state vector
    if (state[i].GetBallot() > best_ballot) {
      best_ballot = state[i].GetBallot();
      best_value = state[i].GetValue();
    }
  }
  ROMULUS_DEBUG("Prepared slot: log_offset={}, state={}", log_offset_,
                curr_proposal->ToString());
  return curr_proposal;
}