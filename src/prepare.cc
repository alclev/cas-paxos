#include "cas_paxos_st.h"
#include "util.h"

using namespace paxos_st;

bool CasPaxos::Prepare() {
  State* curr_proposal = &proposed_state_[prep_offset_];
  Ballot curr_promise_ballot = curr_proposal->GetPromiseBallot();
  if (local_ballot_ == 0) {
    local_ballot_ = MakeBallot(1);
  }
  curr_promise_ballot = std::max(curr_promise_ballot, local_ballot_);
  curr_proposal->SetPromiseBallot(curr_promise_ballot);

  std::vector<State> swap(system_size_);
  std::vector<State> state(system_size_);
  uint32_t done_count = 0;

  // Init
  for (uint32_t i = 0; i < system_size_; ++i) {
    preprepare_expected_[i] = State();
    preprepare_done_[i] = false;
    swap[i] = State(curr_promise_ballot, 0, Value(0));
    state[i] = State();
  }
  uint64_t wr_id_base = (static_cast<uint64_t>(wr_id_) << 48) |
                        (static_cast<uint64_t>(host_id_) << 32);
  uint32_t cached_offset = prep_offset_ * kSlotSize;

  while (done_count < quorum_) {
    ++wr_id_;
    // Post CAS ops.
    int posted = 0;
    for (uint32_t i = 0; i < system_size_; ++i) {
      if (preprepare_done_[i]) continue;
      if (detected_[i]) {
        preprepare_done_[i] = true;
        ++done_count;
        continue;
      }
      uint64_t wr_id = wr_id_base | static_cast<uint64_t>(i);

      auto& conn = preprepare_conns_[i];
      auto& raddr = cached_raddrs_[i];
      raddr.addr_info.offset = cached_offset;
      preprepare_laddr_.offset = i * kSlotSize;

      conn->CompareAndSwap(preprepare_laddr_, raddr, preprepare_expected_[i].raw, swap[i].raw,
                           wr_id);
      posted++;
    }
    preprepare_conns_[0]->ProcessCompletions(posted);
    // Check return value of CAS.
    bool need_bump = false;
    Ballot observed_max_ballot = curr_promise_ballot;

    for (uint32_t i = 0; i < system_size_; ++i) {
      if (preprepare_done_[i]) continue;
      preprepare_laddr_.offset = i * kSlotSize;
      State observed =
          *reinterpret_cast<State*>(preprepare_laddr_.addr + preprepare_laddr_.offset);

      if (observed.raw == preprepare_expected_[i].raw) {
        // ROMULUS_DEBUG("Prepare: cas success");
        state[i] = observed;
        preprepare_done_[i] = true;
        ++done_count;
      } else {
        // ROMULUS_DEBUG(
        //     "Prepare: cas failed but promise ballot still good. observed={}, "
        //     "expected={}",
        //     observed.ToString(), preprepare_expected_[i].ToString());
        preprepare_expected_[i] = observed;
        swap[i] = State(curr_promise_ballot, observed.GetBallot(),
                        observed.GetValue());
        state[i] = observed;
        if (observed.GetPromiseBallot() > curr_promise_ballot) {
          need_bump = true;
          observed_max_ballot =
              std::max(observed_max_ballot, observed.GetPromiseBallot());
        }
      }
    }
    // Handle ballot bump after processing all completions
    if (need_bump) {
      ROMULUS_INFO("Prepare: Need bump triggered. Unimplemented.");
    }
  }
  // Reduce over the quorum: adopt highest accepted proposal
  for (uint32_t i = 0; i < system_size_; ++i) {
    if (state[i].GetBallot() > curr_proposal->GetBallot()) {
      curr_proposal->SetProposal(state[i].GetBallot(), state[i].GetValue());
    }
  }
  ROMULUS_DEBUG("Prepared slot: prep_offset={}, state={}", prep_offset_.load(),
                curr_proposal->ToString());
  prepared_swap_[prep_offset_] = State(curr_promise_ballot, 0, Value(0));
  prep_offset_.fetch_add(1);
  return true;
}