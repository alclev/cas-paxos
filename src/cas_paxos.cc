#include "cas_paxos.h"

CasPaxos::CasPaxos(cons_ctx_t ctx, uint64_t id, uint64_t system_size,
                   uint64_t quorum)
    : cons_ctx_(ctx), id_(id), system_size_(system_size), quorum_(quorum) {}

bool CasPaxos::Prepare(uint32_t shard_id) {
  State* curr_proposal = &proposed_state_[shard_id];
  Ballot curr_promise_ballot =
      BumpBallot(std::max(curr_proposal->GetPromiseBallot(), local_ballot_));
  curr_proposal->SetPromiseBallot(curr_promise_ballot);

  auto backoff =
      std::chrono::microseconds(std::rand() % cas_paxos::kMaxStartingBackoff);

  std::vector<State> swap(system_size_,
                          State(curr_promise_ballot, 0, Value(0)));
  std::vector<State> state(system_size_, State());
  expected_ = std::vector<State>(system_size_, State());
  std::vector<bool> done(system_size_, false);
  uint32_t done_count = 0;

  uint64_t wr_id_base =
      (static_cast<uint64_t>(id_) << 32) | static_cast<uint64_t>(shard_id);
  uint32_t cached_offset = shard_id * cas_paxos::kSlotSize;
  while (done_count < quorum_) {
    // Post CAS ops.
    int posted = 0;
    for (uint32_t n = 0; n < system_size_; ++n) {
      if (done[n]) continue;

      uint64_t wr_id = wr_id_base | static_cast<uint64_t>(n);

      auto& conn = cons_ctx_.conns_[n];
      auto& raddr = cons_ctx_.log_mat_[shard_id][n];
      auto laddr = cons_ctx_.laddr_;

      raddr.addr_info.offset = cached_offset;
      laddr.offset = n * cas_paxos::kSlotSize;

      conn->CompareAndSwap(laddr, raddr, expected_[n].raw, swap[n].raw, wr_id);
      ++posted;
    }

    cons_ctx_.conns_.front()->ProcessCompletions(posted);

    // Check return value of CAS.
    bool need_bump = false;
    Ballot observed_max_ballot = curr_promise_ballot;
    // uint32_t winning_index = 0;

    for (uint32_t n = 0; n < system_size_; ++n) {
      if (done[n]) continue;
      auto laddr = cons_ctx_.laddr_;
      laddr.offset = n * cas_paxos::kSlotSize;
      State observed =
          *reinterpret_cast<State*>(laddr.addr + laddr.offset);

      if (observed.raw == expected_[n].raw) {
        ROMULUS_DEBUG("Prepare: cas success");
        state[n] = observed;
        done[n] = true;
        ++done_count;
      } else {
        ROMULUS_DEBUG(
            "Prepare: cas failed but promise ballot still good. observed={}, "
            "expected={}",
            observed.ToString(), expected_[n].ToString());
        expected_[n] = observed;
        swap[n] = State(curr_promise_ballot, observed.GetBallot(),
                        observed.GetValue());
        state[n] = observed;
        if (observed.GetPromiseBallot() > curr_promise_ballot) {
          need_bump = true;
          observed_max_ballot =
              std::max(observed_max_ballot, observed.GetPromiseBallot());
          // winning_index = n;
        }
      }
    }
    // Handle ballot bump after processing all completions
    if (need_bump) {
      ROMULUS_DEBUG("Prepare: cas failed. abort and bump.");
      Ballot unique_ballot = BumpBallot(observed_max_ballot);
      ROMULUS_ASSERT(unique_ballot > observed_max_ballot,
                     "GlobalBallot did not exceed observed promise ballot");
      curr_promise_ballot = unique_ballot;
      curr_proposal->SetPromiseBallot(unique_ballot);
      // reset metadata
      done_count = 0;
      std::fill(done.begin(), done.end(), false);
      std::fill(expected_.begin(), expected_.end(), State());
      std::fill(swap.begin(), swap.end(),
                State(curr_promise_ballot, 0, Value(0)));
      // perform backoff
      backoff = DoBackoff(backoff);
    }
  }
  // Reduce over the quorum: adopt highest accepted proposal
  Ballot best_ballot = 0;
  Value best_value = Value(0);

  for (uint32_t i = 0; i < system_size_; ++i) {
    if (!done[i]) continue;
    // we reduce over the state vector
    if (state[i].GetBallot() > best_ballot) {
      best_ballot = state[i].GetBallot();
      best_value = state[i].GetValue();
    }
  }
  ROMULUS_DEBUG("Prepared slot: shard_id={}, state={}", shard_id,
                curr_proposal->ToString());

  // seed expected for promise phase
  expected_ = std::move(swap);
  return true;
}

bool CasPaxos::Promise(uint32_t shard_id, Value v) {
  State* curr_proposal = &proposed_state_[shard_id];
  Ballot curr_promise_ballot = curr_proposal->GetPromiseBallot();
  uint32_t done_count = 0;

  // expected already set from prepare
  std::vector<bool> done(system_size_, false);

  // We install the chose value
  curr_proposal->SetProposal(curr_promise_ballot, v);

  uint32_t cached_offset = shard_id * cas_paxos::kSlotSize;
  uint64_t wr_id_base =
      (static_cast<uint64_t>(id_) << 32) | static_cast<uint64_t>(shard_id);

  // Retry until a quroum succeeds and the local log is written to. Making
  // sure that we write to the local log allows a follower to be certain
  // that if the slot is filled that the value is committed.
  int posted = 0;
  while (done_count < quorum_) {
    // Post CAS ops.
    posted = 0;
    for (uint32_t n = 0; n < system_size_; ++n) {
      // Already succeeded.
      if (done[n]) continue;

      // Post a request
      auto& conn = cons_ctx_.conns_[n];
      auto& raddr = cons_ctx_.log_mat_[shard_id][n];
      raddr.addr_info.offset = cached_offset;
      auto& laddr = cons_ctx_.laddr_;
      laddr.offset = n * cas_paxos::kSlotSize;

      uint64_t wr_id = wr_id_base | static_cast<uint64_t>(n);

      conn->CompareAndSwap(laddr, raddr, expected_[n].raw, curr_proposal->raw,
                           wr_id);
      ++posted;
    }

    // Shared cq batch poll
    cons_ctx_.conns_.front()->ProcessCompletions(posted);

    for (uint32_t n = 0; n < system_size_; ++n) {
      if (done[n]) continue;
      // This will the result of the previous CAS
      auto laddr = cons_ctx_.laddr_;
      laddr.offset = n * cas_paxos::kSlotSize;
      State observed =
          *reinterpret_cast<State*>(laddr.addr + laddr.offset);
      // State observed = *c->scratch_state;

      if (expected_[n].raw == observed.raw) {
        // ROMULUS_DEBUG(
        //     "<Promise> CAS success! observed={}, expected={}, "
        //     "curr_proposal={}",
        //     observed.ToString(), expected_[i].ToString(),
        //     curr_proposal->ToString());
        // CAS succeeded. Done.
        done[n] = true;
        ++done_count;
      } else if (observed.GetPromiseBallot() > curr_promise_ballot) {
        // ROMULUS_DEBUG(
        //     "<Promise> CAS failure Case 1: Seen higher ballot, abort."
        //     "observed={}, expected={}, curr_proposal={}",
        //     observed.ToString(), expected_[i].ToString(),
        //     curr_proposal->ToString());
        return false;
      } else {
        // ROMULUS_DEBUG(
        //     "<Promise> CAS failure Case 2: Ballot still good. retry."
        //     "observed={}, expected={}, curr_proposal={}",
        //     observed.ToString(), expected_[i].ToString(),
        //     curr_proposal->ToString());
        expected_[n] = observed;
      }
    }
  }
  installed_promise_ = curr_proposal;
  return true;
}

Ballot CasPaxos::MakeBallot(uint32_t round) {
  uint32_t b = round * system_size_ + id_;
  ROMULUS_ASSERT(b <= std::numeric_limits<uint16_t>::max(),
                 "Ballot overflow: round={}, system_size={}", round,
                 system_size_);
  return static_cast<Ballot>(b);
}

Ballot CasPaxos::BumpBallot(Ballot observed_ballot) {
  auto observed_round = observed_ballot / system_size_;
  auto my_round = local_ballot_ / system_size_;
  auto next_round = std::max(observed_round + 1, my_round + 1);

  auto new_ballot = MakeBallot(next_round);
  local_ballot_ = static_cast<Ballot>(new_ballot);
  return local_ballot_;
}
