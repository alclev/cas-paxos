#include "cas_paxos_st.h"

using namespace paxos_st;

bool CasPaxos::Promise(Value v) {
#ifdef PROMISE_BENCH
  std::chrono::_V2::steady_clock::time_point t0, t1, t2, t3, t4;
  t0 = std::chrono::steady_clock::now();
#endif
  State* curr_proposal = &proposed_state_[log_offset_];
  Ballot curr_promise_ballot = curr_proposal->GetPromiseBallot();
  RemoteContext* c;
  uint32_t done_count = 0;

  // Metadata to indicate to indicate a SUCCESSFUL cas for the given
  // acceptor
  // Metadata to indicate whether a CAS for a given acceptor has been POLLED
  std::fill(expected_.begin(), expected_.end(), State());
  std::fill(done_.begin(), done_.end(), false);

  // We install the chose value if it hasn't already been set
  if (curr_proposal->GetBallot() == 0) {
    curr_proposal->SetProposal(curr_promise_ballot, v);
  }

  ROMULUS_DEBUG("<Promise> slot={}, state={}", log_offset_,
                proposed_state_[log_offset_].ToString());

  // Retry until a quroum succeeds and the local log is written to. Making
  // sure that we write to the local log allows a follower to be certain
  // that if the slot is filled that the value is committed.
  while (done_count < quorum_) {
#ifdef PROMISE_BENCH
    t1 = std::chrono::steady_clock::now();
#endif
    // Post CAS ops.
    std::vector<uint64_t> posted;
    posted.reserve(system_size_ - 1);
    for (uint32_t i = 0; i < system_size_; ++i) {
      // Already succeeded.
      if (done_[i]) continue;
      if (detected_[i]) {
        done_[i] = true;
        ++done_count;
        continue;
      }
      // Post a request
      c = contexts_[i];
      c->log_raddr.addr_info.offset = log_offset_ * kSlotSize;

      uint64_t wr_id = (static_cast<uint64_t>(wr_id_) << 48) |
                       (static_cast<uint64_t>(host_id_) << 32) |
                       static_cast<uint64_t>(i);
      romulus::WorkRequest::BuildCAS(c->scratch_laddr, c->log_raddr,
                                     expected_[i].raw, curr_proposal->raw,
                                     wr_id, &c->wr);
      ROMULUS_ASSERT(c->conn->Post(&c->wr, 1),
                     "<Promise> Failed when posting requests.");
      if (i == host_id_) {
        // use the loopback cq instead of shared
        ROMULUS_ASSERT(c->conn->ProcessCompletions(1) == 1,
                       "<Promise> Failed when polling for completions.");
      } else {
        posted.push_back(wr_id);
      }
    }
    int not_me = (host_id_ + 1) % system_size_;
    while (detected_[not_me]) {
      not_me = (not_me + 1) % system_size_;
    }
    ROMULUS_ASSERT(
        contexts_[not_me]->conn->PollBatch(posted) == (int)posted.size(),
        "<Promise> Failed when polling for completions.");

#ifdef PROMISE_BENCH
    t2 = std::chrono::steady_clock::now();
#endif

    // uint32_t completions = 0;
    // while (completions < posted) {
    //   for (uint32_t i = 0; i < system_size_; ++i) {
    //     if (done_[i] || polled_[i]) continue;
    //     c = contexts_[i];
    //     if (PollCompletionsOnce(c->conn, wr_ids_[i])) {
    //       polled_[i] = true;
    //       ++completions;
    //     }
    //   }
    // }
#ifdef PROMISE_BENCH
    t3 = std::chrono::steady_clock::now();
#endif
    for (uint32_t i = 0; i < system_size_; ++i) {
      if (done_[i]) continue;
      c = contexts_[i];
      // This will the result of the previous CAS
      State observed = *c->scratch_state;

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
#ifdef PROMISE_BENCH
    t4 = std::chrono::steady_clock::now();
#endif
    ++wr_id_;
  }
#ifdef PROMISE_BENCH
  promise_bench_times_.push_back(
      {static_cast<double>(
           std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0)
               .count()),
       static_cast<double>(
           std::chrono::duration_cast<std::chrono::nanoseconds>(t2 - t1)
               .count()),
       static_cast<double>(
           std::chrono::duration_cast<std::chrono::nanoseconds>(t3 - t2)
               .count()),
       static_cast<double>(
           std::chrono::duration_cast<std::chrono::nanoseconds>(t4 - t3)
               .count()),
       static_cast<double>(
           std::chrono::duration_cast<std::chrono::nanoseconds>(t4 - t0)
               .count())});
#endif
  log_[log_offset_] = *curr_proposal;
  return true;
}