#include "cas_paxos_st.h"

using namespace paxos_st;

void CasPaxos::CatchUp() {
  if (is_leader_) return;
  auto start = std::chrono::steady_clock::now();
  int iterations = 0;
  while (TryCatchUp()) {
    iterations++;
    if (failover_detected_) {
      auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count();
      ROMULUS_INFO(
          "<CatchUp> Broke out due to failover after {} iterations, {} us",
          iterations, elapsed);
      break;
    }
  }
}

/// @brief Issues RMDA-reads to all replicas, busy-waits until all reads
/// complete. Scan's scratch buffers for quorum agreement -- if committed,
/// ammend local log with RDMA CAS
/// @return bool
bool CasPaxos::TryCatchUp() {
  // ROMULUS_DEBUG("<TryCatchUp> Catching up slot: {}", log_offset_);
  // Post READs to all remote peers.
  RemoteContext* c;
  for (uint32_t i = 0; i < system_size_; ++i) {
    if (detected_[i]) continue;
    if (failover_detected_) return false;

    c = contexts_[i];

    c->log_raddr.addr_info.offset = log_offset_ * kSlotSize;
    uint64_t wr_id = (static_cast<uint64_t>(wr_id_) << 48) |
                     (static_cast<uint64_t>(host_id_) << 32) |
                     static_cast<uint64_t>(i);

    c->conn->Read(c->scratch_laddr, c->log_raddr, wr_id);
    if (c->conn->ProcessCompletions(1) != 1) {
      ROMULUS_DEBUG("<TryCatchUp> Failed to poll for completion of RDMA read");
      return false;
    }
  }


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
    if (failover_detected_) return false;
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
        loopback_context->conn->Read(loopback_context->scratch_laddr,
                                     loopback_context->log_raddr, wr_id_);
        ROMULUS_ASSERT(loopback_context->conn->ProcessCompletions(1) == 1,
                       "<TryCatchUp> Failed to poll for completion of loopback "
                       "RDMA read.");
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