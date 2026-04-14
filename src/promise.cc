#include "cas_paxos_st.h"

using namespace paxos_st;

bool CasPaxos::Promise(Value v) {
  State* curr_proposal = &proposed_state_[log_offset_];
  Ballot curr_promise_ballot = curr_proposal->GetPromiseBallot();
  uint32_t done_count = 0;

  // Metadata to indicate to indicate a SUCCESSFUL cas for the given
  // acceptor
  // Metadata to indicate whether a CAS for a given acceptor has been POLLED
  for (uint32_t i = 0; i < system_size_; ++i) {
    expected_[i] = State();
    done_[i] = false;
  }

  // We install the chose value if it hasn't already been set
  if (curr_proposal->GetBallot() == 0) {
    curr_proposal->SetProposal(curr_promise_ballot, v);
  }

  // ROMULUS_DEBUG("<Promise> slot={}, state={}", log_offset_,
  //               proposed_state_[log_offset_].ToString());

  // cached values
  uint32_t cached_offset = log_offset_ * kSlotSize;
  uint64_t wr_id_base = (static_cast<uint64_t>(wr_id_) << 48) |
                        (static_cast<uint64_t>(host_id_) << 32);
  // Retry until a quroum succeeds and the local log is written to. Making
  // sure that we write to the local log allows a follower to be certain
  // that if the slot is filled that the value is committed.
  int posted = 0;
  while (done_count < quorum_) {
    // Post CAS ops.
    posted = 0;
    for (uint32_t i = 0; i < system_size_; ++i) {
      // Already succeeded.
      if (done_[i]) continue;
      if (detected_[i]) {
        done_[i] = true;
        ++done_count;
        continue;
      }
      // Post a request
      auto& conn = cached_conns_[i];
      auto& raddr = cached_raddrs_[i];
      raddr.addr_info.offset = cached_offset;
      auto& laddr = cached_laddr_;
      laddr.offset = i * kSlotSize;

      uint64_t wr_id = wr_id_base | static_cast<uint64_t>(i);

      conn->CompareAndSwap(laddr, raddr, expected_[i].raw,
                           curr_proposal->raw, wr_id);
      ++posted;
    }

    // Shared cq batch poll
    remote_conns_[0][0]->ProcessCompletions(posted);

    for (uint32_t i = 0; i < system_size_; ++i) {
      if (done_[i]) continue;
      // This will the result of the previous CAS
      cached_laddr_.offset = i * kSlotSize;
      State observed = *reinterpret_cast<State*>(cached_laddr_.addr + cached_laddr_.offset);
      // State observed = *c->scratch_state;

      if (expected_[i].raw == observed.raw) {
        // ROMULUS_DEBUG(
        //     "<Promise> CAS success! observed={}, expected={}, "
        //     "curr_proposal={}",
        //     observed.ToString(), expected_[i].ToString(),
        //     curr_proposal->ToString());
        // CAS succeeded. Done.
        done_[i] = true;
        ++done_count;
      } else if (observed.GetPromiseBallot() > curr_promise_ballot) {
        // ROMULUS_DEBUG(
        //     "<Promise> CAS failure Case 1: Seen higher ballot, abort."
        //     "observed={}, expected={}, curr_proposal={}",
        //     observed.ToString(), expected_[i].ToString(),
        //     curr_proposal->ToString());
        // We will make an assumption the that
        stable_leader_ = true;
        return false;
      } else {
        // ROMULUS_DEBUG(
        //     "<Promise> CAS failure Case 2: Ballot still good. retry."
        //     "observed={}, expected={}, curr_proposal={}",
        //     observed.ToString(), expected_[i].ToString(),
        //     curr_proposal->ToString());
        expected_[i] = observed;
      }
    }

    ++wr_id_;
  }

  log_[log_offset_] = *curr_proposal;
  return true;
}
