#include "cas_paxos_st.h"
#include "util.h"

#define FAILURE_THRESHOLD 15
#define WR_FAILIRE_THRESHOLD 1
constexpr uint64_t PULSE_WAIT_US = 1; // 100

using namespace paxos_st;

void CasPaxos::Failover(int node_id) {
  int leader_id = leader_.load().GetPromiseBallot() % system_size_;
  if (node_id == leader_id) {
    ROMULUS_INFO(
        "[FAILURE DETECTOR] Leader {} is suspected to have failed. "
        "New leader will be {}.",
        node_id, new_leader_id_.load());
    stable_leader_ = false;
    new_leader_id_ = (node_id + 1) % system_size_;
    failover_start_time_ = std::chrono::steady_clock::now();
    failover_detected_ = true;

  } else {
    ROMULUS_INFO("[FAILURE DETECTOR] Machine {} is suspected to have failed.",
                 node_id);
  }
}

std::vector<std::thread> CasPaxos::FailureDetector() {
  ROMULUS_INFO("Activating failure detector...");
  ReadLeaderSlot();

  std::thread loopback_thread(&CasPaxos::loopback_heartbeat, this);
  loopback_thread.detach();

  std::vector<std::thread> fd_threads;
  for (int t = 0; t < system_size_; t++) {
    if (t == host_id_) continue;
    fd_threads.emplace_back(&CasPaxos::peer_heartbeat, this, t);
  }

  return fd_threads;
}

void CasPaxos::peer_heartbeat(int tid) {
  uint64_t curr_heartbeat = 0;
  uint64_t prev_heartbeat = 0;
  uint64_t score = 0;
  uint64_t wr_failure_count = 0;

  auto* conn = remote_conns_[tid][tid + 1];
  ROMULUS_ASSERT(conn, "Connection for failure detector thread is invalid");

  // update our own counter via loopback
  auto laddr = memblock_.GetAddrInfo(kFailureDetectorId + "_thread_" +
                                     std::to_string(tid));
  laddr.offset = 0;
  laddr.length = kSlotSize;
  auto raddr = remote_addrs_[tid][kHeartBeatRegionId];
  raddr.addr_info.offset = 0;
  raddr.addr_info.length = kSlotSize;

  while (failure_detector_running_.load()) {
    if (detected_[tid]) return;

    // ROMULUS_DEBUG("Reading heartbeat on conn {}",
    //               reinterpret_cast<uintptr_t>(conn));
    uint64_t wr_id = (static_cast<uint64_t>(wr_id_) << 48) |
                     (static_cast<uint64_t>(host_id_) << 32) |
                     static_cast<uint64_t>(tid);
    conn->Read(laddr, raddr, wr_id);

    // replace with raw polling
    if (conn->ProcessCompletions(1) != 1) {
      ROMULUS_DEBUG("Error polling in failure detector for machine {}.", tid);
      // Note: aggressive failure detection -- zero tolerance for missed
      // completions
      wr_failure_count++;
      if (wr_failure_count > WR_FAILIRE_THRESHOLD) {
        ROMULUS_INFO(
            "Exceeded RDMA read failure threshold for machine {}. Marking as "
            "failed.",
            tid);
        detected_[tid] = true;
        Failover(tid);
        return;
      }
      continue;
    }

    uint64_t peer_heartbeat =
        *reinterpret_cast<uint64_t*>(laddr.addr + laddr.offset);
    curr_heartbeat = peer_heartbeat;

    if (prev_heartbeat == 0) {
      prev_heartbeat = curr_heartbeat;
      continue;
    }
    // interpret
    if (curr_heartbeat == prev_heartbeat) {
      score++;
      // ROMULUS_DEBUG("[HEARTBEAT] Peer {} heartbeat unchanged at {}. Score:
      // {}",
      //               tid, curr_heartbeat, score);
    } else {
      score = 0;
    }

    if (score > FAILURE_THRESHOLD && !detected_[tid]) {
      detected_[tid] = true;
      Failover(tid);
    }

    prev_heartbeat = curr_heartbeat;
    std::this_thread::sleep_for(std::chrono::microseconds(PULSE_WAIT_US));
  }
}

void CasPaxos::loopback_heartbeat() {
  uint64_t heartbeat_counter = 0;

  // Extra loopback
  auto* loopback = remote_conns_[host_id_][1];

  ROMULUS_ASSERT(loopback,
                 "Connection for loopback failure detector thread is invalid");

  // update our own counter via loopback
  auto laddr = memblock_.GetAddrInfo(kFailureDetectorId + "_thread_" +
                                     std::to_string(host_id_));
  laddr.offset = 0;
  laddr.length = kSlotSize;
  auto raddr = remote_addrs_[host_id_][kHeartBeatRegionId];
  raddr.addr_info.offset = 0;
  raddr.addr_info.length = kSlotSize;

  while (failure_detector_running_.load()) {
    heartbeat_counter++;
    *reinterpret_cast<uint64_t*>(laddr.addr + laddr.offset) = heartbeat_counter;

    // Assuing that this qp has its own cq
    loopback->Write(laddr, raddr, wr_id_);
    if (loopback->ProcessCompletions(1) != 1) {
      ROMULUS_DEBUG("Error polling in loopback failure detector.");
      continue;
    }
    std::this_thread::sleep_for(std::chrono::microseconds(PULSE_WAIT_US));
  }
}

bool CasPaxos::BroadcastLeader(State* new_leader) {
  // uint32_t ok_count = 0;
  std::vector<bool> ok(system_size_);
  auto laddr = memblock_.GetAddrInfo(kScratchRegionId + "_0");
  auto* laddr_raw = reinterpret_cast<uint8_t*>(laddr.addr);

  // Write the new leader meta-data to everyone's slot, including my own
  for (int i = 0; i < system_size_; i++) {
    auto* c = contexts_[i];
    if (detected_[i]) continue;

    laddr.offset = i * kSlotSize;
    laddr.length = kSlotSize;
    *reinterpret_cast<State*>(laddr_raw + laddr.offset) = *new_leader;

    auto raddr = remote_addrs_[i][kLeaderRegionId];
    raddr.addr_info.offset = 0;
    raddr.addr_info.length = kSlotSize;
    // We hardcode using the first qp, unless there is an explicit reason to
    // iterate through them on a single thread
    c->conn = remote_conns_[i].front();
    // NB: Here, we can consider chaining the WR's and passing them to a
    // single connection but the problem here is that we are interfacing
    // with multiple peers, so we cannot, rely on a single QP, therefore we
    // will have to post one
    // if (i != 0) {
    //   outstanding_[i - 1].append(&outstanding_[i]);
    // }
    uint64_t wr_id = (static_cast<uint64_t>(wr_id_) << 48) |
                     (static_cast<uint64_t>(host_id_) << 32) |
                     static_cast<uint64_t>(i);
    c->conn->Write(laddr, raddr, wr_id);
    ROMULUS_ASSERT(c->conn->ProcessCompletions(1) == 1,
                   "Error polling in leader change.");
  }
  return true;
}

void CasPaxos::ReadLeaderSlot() {
  std::thread([this]() {
    // Landing space for the reads
    // we only need to read once from our own slot
    auto laddr = memblock_.GetAddrInfo(kScratchRegionId + "_2");
    laddr.offset = host_id_ * kSlotSize;
    laddr.length = kSlotSize;

    auto raddr = remote_addrs_[host_id_][kLeaderRegionId];
    raddr.addr_info.offset = 0;
    raddr.addr_info.length = kSlotSize;

    // Should be loopback #3
    auto* loopback_conn = remote_conns_[host_id_].back();

    while (failure_detector_running_.load()) {
      loopback_conn->Read(laddr, raddr, wr_id_);
      if (loopback_conn->ProcessCompletions(1) != 1) {
        ROMULUS_FATAL("Error polling in leader read.");
      }
      State read_leader = *reinterpret_cast<State*>(laddr.addr + laddr.offset);

      if (read_leader != leader_.load()) {
        ROMULUS_INFO("[Leader READ] Detected leader change: {}",
                     ExtractId(read_leader));
        leader_.store(read_leader);
      }

      std::this_thread::sleep_for(std::chrono::microseconds(PULSE_WAIT_US));
    }
  }).detach();
}