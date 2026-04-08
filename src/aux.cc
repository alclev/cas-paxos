#include "cas_paxos_st.h"

using namespace paxos_st;

// decode the ballot (round * sys_size + host_id)
// Note, the division gets rid of the host_id constant
Ballot CasPaxos::BumpBallot(Ballot observed_ballot) {
  auto observed_round = observed_ballot / system_size_;
  auto my_round = local_ballot_ / system_size_;
  auto next_round = std::max(observed_round + 1, my_round + 1);

  auto new_ballot = MakeBallot(next_round);
  local_ballot_ = static_cast<Ballot>(new_ballot);
  return local_ballot_;
}

Ballot CasPaxos::MakeBallot(uint32_t round) {
  uint32_t b = round * system_size_ + host_id_;
  ROMULUS_ASSERT(b <= std::numeric_limits<uint16_t>::max(),
                 "Ballot overflow: round={}, system_size={}", round,
                 system_size_);
  return static_cast<Ballot>(b);
}

Ballot CasPaxos::GlobalBallot() {
  uint64_t epoch = 0;
  registry_->Fetch_and_Add("paxos_epoch", 1, &epoch);
  // This allows for a maximum of 15 proposers
  ROMULUS_ASSERT(epoch < (1ULL << 12), "Ballot overflow");
  epoch = (epoch << 4) | host_id_;
  // truncate the first 48 bits of the word
  return static_cast<Ballot>(epoch);
}

void CasPaxos::ConditionalReset() {
  if (log_offset_ >= kRingSize) {
    log_offset_ = 0;
    Ballot winning_ballot = proposed_state_[0].GetPromiseBallot();

    for (uint32_t i = 0; i < kRingSize; ++i) {
      proposed_state_[i] = State(winning_ballot, kNullBallot, kNullValue);
      log_[i] = State(0, kNullBallot, kNullValue);
    }

    for (int i = 0; i < system_size_; i++) {
      auto* c = contexts_[i];
      if (detected_[i]) continue;
      auto laddr = memblock_.GetAddrInfo(kScratchRegionId);
      auto laddr_st = reinterpret_cast<State*>(laddr.addr);
      *laddr_st = State(winning_ballot, kNullBallot, kNullValue);

      for (uint32_t slot = 0; slot < kRingSize; ++slot) {
        auto raddr = remote_addrs_[i][kProposedRegionId];
        raddr.addr_info.offset = slot * kSlotSize;
        raddr.addr_info.length = kSlotSize;
        uint64_t wr_id = (static_cast<uint64_t>(wr_id_) << 48) |
                         (static_cast<uint64_t>(host_id_) << 32) |
                         static_cast<uint64_t>(slot);
        romulus::WorkRequest::BuildWrite(laddr, raddr, wr_id, &c->wr);
        ROMULUS_ASSERT(
            c->conn->Post(&c->wr, 1),
            "<ConditionalReset> Failed to post requests when resetting "
            "proposed region.");
        c->conn->PollBatch({wr_id});
      }

      *laddr_st = State(0, kNullBallot, kNullValue);

      for (uint32_t slot = 0; slot < kRingSize; ++slot) {
        auto raddr = remote_addrs_[i][kLogRegionId];
        raddr.addr_info.offset = slot * kSlotSize;
        raddr.addr_info.length = kSlotSize;
        uint64_t wr_id = (static_cast<uint64_t>(wr_id_) << 48) |
                         (static_cast<uint64_t>(host_id_) << 32) |
                         static_cast<uint64_t>(slot);
        romulus::WorkRequest::BuildWrite(laddr, raddr, wr_id, &c->wr);
        ROMULUS_ASSERT(
            c->conn->Post(&c->wr, 1),
            "<ConditionalReset> Failed to post requests when resetting "
            "log region.");
        c->conn->PollBatch({wr_id});
      }
    }
  }
}

void CasPaxos::ClearLogs() {
  ROMULUS_DEBUG("<ClearLogs> Clearing logs starting at log_offset={}",
                log_offset_);
  for (uint32_t i = 0; i < kRingSize; ++i) {
    proposed_state_[i] = State(0, kNullBallot, kNullValue);
    log_[i] = State(0, kNullBallot, kNullValue);
  }

  for (int i = 0; i < system_size_; i++) {
    if (detected_[i]) continue;

    auto conn = remote_conns_[i][1];
    auto laddr = memblock_.GetAddrInfo(kScratchExtraRegionId);
    auto laddr_st = reinterpret_cast<State*>(laddr.addr);
    *laddr_st = State(0, kNullBallot, kNullValue);

    for (uint32_t slot = log_offset_; slot < kRingSize; ++slot) {
      auto raddr = remote_addrs_[i][kProposedRegionId];
      raddr.addr_info.offset = slot * kSlotSize;
      raddr.addr_info.length = kSlotSize;
      uint64_t wr_id = (static_cast<uint64_t>(wr_id_) << 48) |
                       (static_cast<uint64_t>(host_id_) << 32) |
                       static_cast<uint64_t>(slot);
      conn->Write(laddr, raddr, wr_id);
      ROMULUS_ASSERT(conn->ProcessCompletions(1) == 1,
                     "<ConditionalReset> Failed when polling for completions.");
    }

    *laddr_st = State(0, kNullBallot, kNullValue);

    for (uint32_t slot = log_offset_; slot < kRingSize; ++slot) {
      auto raddr = remote_addrs_[i][kLogRegionId];
      raddr.addr_info.offset = slot * kSlotSize;
      raddr.addr_info.length = kSlotSize;
      uint64_t wr_id = (static_cast<uint64_t>(wr_id_) << 48) |
                       (static_cast<uint64_t>(host_id_) << 32) |
                       static_cast<uint64_t>(slot);
      conn->Write(laddr, raddr, wr_id);
      ROMULUS_ASSERT(conn->ProcessCompletions(1) == 1,
                     "<ConditionalReset> Failed when polling for completions.");
    }
  }
}

void CasPaxos::CleanUp() {
  ROMULUS_INFO("Cleaning up...");
#ifdef PROMISE_BENCH
  // calculate the average time for each phase
  std::vector<double> avg_times(5, 0);
  for (const auto& times : promise_bench_times_) {
    for (size_t i = 0; i < times.size(); ++i) {
      avg_times[i] += times[i];
    }
  }
  for (size_t i = 0; i < avg_times.size(); ++i) {
    avg_times[i] /= promise_bench_times_.size();
  }
  ROMULUS_INFO(
      "Average Promise times (ns): init={} post={} poll={} check={} total={}",
      avg_times[0], avg_times[1], avg_times[2], avg_times[3], avg_times[4]);

#endif

  running_.store(false);
  if (is_leader_) {
    State shutdown_msg(0, 0, Value(kShutdown));
    BroadcastLeader(&shutdown_msg);
  }
  CatchUp();
  // ROMULUS_COUNTER_ACC("p1_aborts");
  // ROMULUS_COUNTER_ACC("p2_aborts");
  // ROMULUS_COUNTER_ACC("attempts");
  // ROMULUS_COUNTER_ACC("skipped");
  // ROMULUS_COUNTER_ACC("proposed");
  // ROMULUS_INFO("!> p1_aborts={}", ROMULUS_COUNTER_GET("p1_aborts"));
  // ROMULUS_INFO("!> p2_aborts={}", ROMULUS_COUNTER_GET("p2_aborts"));
  // ROMULUS_INFO("!> attempts={}", ROMULUS_COUNTER_GET("attempts"));
  // ROMULUS_INFO("!> skipped={}", ROMULUS_COUNTER_GET("skipped"));
  // ROMULUS_INFO("!> proposed={}", ROMULUS_COUNTER_GET("proposed"));
}

void CasPaxos::SyncNodes() {
  ROMULUS_DEBUG("Syncing nodes");
  conn_manager_->arrive_strict_barrier();
  ROMULUS_DEBUG("Nodes synced.");
}

void CasPaxos::Reset() {
  log_offset_ = 0;
  buf_offset_ = 0;
}