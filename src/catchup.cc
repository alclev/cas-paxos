#include "cas_paxos_st.h"

using namespace paxos_st;

void CasPaxos::CatchUp() {
  if (is_leader_) return;
  ROMULUS_DEBUG("<CatchUp> Catching up");
  while (TryCatchUp()) {
    ROMULUS_COUNTER_INC("skipped");
  }
  // Stuck, re-read leader in case it changed
  leader_ = ReadLeaderSlot();
}

/// @brief Issues RMDA-reads to all replicas, busy-waits until all reads
/// complete. Scan's scratch buffers for quorum agreement -- if committed,
/// ammend local log with RDMA CAS
/// @return bool
bool CasPaxos::TryCatchUp() {
  ROMULUS_DEBUG("<TryCatchUp> Catching up slot: {}", log_offset_);
  if (failover_detected_) {
    ROMULUS_DEBUG("<TryCatchUp> Failover detected, bailing catchup.");
    return false;
  }
  // Post READs to all remote peers.
  std::vector<uint64_t> posted;
  posted.reserve(system_size_);
  RemoteContext* c;
  for (uint32_t i = 0; i < system_size_; ++i) {
    if (detected_[i]) continue;
    c = contexts_[i];
    c->log_raddr.addr_info.offset = log_offset_ * kSlotSize;
    uint64_t wr_id = (static_cast<uint64_t>(wr_id_) << 48) |
                     (static_cast<uint64_t>(host_id_) << 32) |
                     static_cast<uint64_t>(i);
    romulus::WorkRequest::BuildRead(c->scratch_laddr, c->log_raddr, wr_id,
                                    &c->wr);
    // StageLogRequest(c);
    ROMULUS_ASSERT(c->conn->Post(&c->wr, 1),
                   "<TryCatchUp> Failed to post requests.");
    if (i == host_id_) {
      ROMULUS_ASSERT(c->conn->ProcessCompletions(1) == 1,
                     "<TryCatchUp> Failed to poll for local read completion.");
    } else {
      posted.push_back(wr_id);
    }
  }
  uint64_t not_me = (host_id_ + 1) % system_size_;
  while (detected_[not_me]) {
    not_me = (not_me + 1) % system_size_;
  }
  contexts_[not_me]->conn->PollBatch(posted);

  // Wait for a response from all nodes.
  //+ Timeout if this takes too long.
  // while (ok_count < system_size_) {
  //   for (uint32_t i = 0; i < system_size_; ++i) {
  //     if (ok[i]) continue;
  //     c = contexts_[i];
  //     ok[i] = PollCompletionsOnce(c->conn, wr_id_);
  //     if (ok[i]) ++ok_count;
  //   }
  // }

  // Compare returned values to determine if this slot has been committed
  // already.
  RemoteContext *c_i, *c_j;
  uint32_t num_agreed = 0;
  Value accepted_val;
  for (uint32_t i = 0; i < quorum_; ++i) {
    c_i = contexts_[i];
    if (c_i->scratch_state->GetBallot() == kNullBallot) continue;
    num_agreed = 1;
    accepted_val = c_i->scratch_state->GetValue();
    for (uint32_t j = i + 1; j < system_size_ && num_agreed < quorum_; ++j) {
      c_j = contexts_[j];
      if (c_j->scratch_state->GetBallot() == kNullBallot) continue;
      if (c_j->scratch_state->GetValue() == accepted_val) {
        ++num_agreed;
      }
    }

    // This slot is committed.
    if (num_agreed >= quorum_) {
      // If the local log does not reflect the accepted value then CAS it
      // in.
      if (log_[log_offset_].GetValue() != accepted_val) {
        auto loopback_context = contexts_[host_id_];
        loopback_context->log_raddr.addr_info.offset = log_offset_ * kSlotSize;
        romulus::WorkRequest::BuildCAS(
            loopback_context->scratch_laddr, loopback_context->log_raddr,
            log_[log_offset_].raw, c_i->scratch_state->raw, wr_id_,
            &loopback_context->wr);
        // StageLogRequest(loopback_context);
        ROMULUS_ASSERT(
            loopback_context->conn->Post(&loopback_context->wr, 1),
            "<TryCatchUp> Failed to post requests when updating local "
            "slot.");

        // Only expect a single outstanding completion.
        std::vector<uint64_t> loopback_wr = {wr_id_};
        loopback_context->conn->PollBatch(loopback_wr);
      }
      ROMULUS_DEBUG("<TryCatchUp> Caught up: log_offset={}, state={}",
                    log_offset_, log_[log_offset_].ToString());
      ++log_offset_;
      ++wr_id_;
      return true;
    }
  }
  ++wr_id_;
  ROMULUS_DEBUG("<TryCatchUp> Failed. log_offset={}", log_offset_);
  return false;
}