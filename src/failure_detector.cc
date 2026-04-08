#include "cas_paxos_st.h"
#include "util.h"

using namespace paxos_st;

void CasPaxos::FailureDetector() {
    ROMULUS_INFO("Activating failure detector...");
    // Initialize our own region to zero
    auto laddr = memblock_.GetAddrInfo(kScratchExtraRegionId);
    laddr.offset = host_id_ * kSlotSize;
    laddr.length = kSlotSize;
    // put zero in staging buffer
    *reinterpret_cast<uint64_t*>(laddr.addr + laddr.offset) = 0;

    auto raddr = remote_addrs_[host_id_][kHeartBeatRegionId];
    raddr.addr_info.offset = 0;
    raddr.addr_info.length = kSlotSize;

    auto extra_loopback = remote_conns_[host_id_][1];
    extra_loopback->Write(laddr, raddr, wr_id_);
    ROMULUS_ASSERT(extra_loopback->ProcessCompletions(1) == 1,
                   "Error polling in failure detector.");

    conn_manager_->arrive_strict_barrier();

    std::thread([this, extra_loopback]() {
      uint64_t heartbeat_counter = 0;
      std::vector<uint64_t> peer_heartbeats(system_size_, 0);
      std::vector<uint64_t> old_peer_heartbeats = {};
      std::vector<uint64_t> scores(system_size_, 0);

      while (running_.load()) {
        // update our own counter via loopback
        auto laddr = memblock_.GetAddrInfo(kScratchExtraRegionId);
        laddr.offset = host_id_ * kSlotSize;
        laddr.length = kSlotSize;

        heartbeat_counter++;
        *reinterpret_cast<uint64_t*>(laddr.addr + laddr.offset) =
            heartbeat_counter;

        auto raddr = remote_addrs_[host_id_][kHeartBeatRegionId];

        raddr.addr_info.offset = 0;
        raddr.addr_info.length = kSlotSize;

        // Assuing that this qp has its own cq
        extra_loopback->Write(laddr, raddr, wr_id_);
        if (extra_loopback->ProcessCompletions(1) != 1) {
          ROMULUS_DEBUG("Error polling in failure detector.");
          continue;
        }

        // ROMULUS_DEBUG("[HEARTBEAT] Updated heartbeat counter to {}.",
        //               heartbeat_counter);

        // read everyone else's counter
        for (int n = 0; n < system_size_; ++n) {
          if (n == host_id_) continue;
          if (detected_[n]) continue;

          laddr.offset = n * kSlotSize;

          auto raddr = remote_addrs_[n][kHeartBeatRegionId];
          raddr.addr_info.offset = 0;
          raddr.addr_info.length = kSlotSize;

          auto conn = remote_conns_[n][1];
          ROMULUS_DEBUG("Reading heartbeat on conn {}", reinterpret_cast<uintptr_t>(conn));
          uint64_t wr_id = (static_cast<uint64_t>(wr_id_) << 48) |
                           (static_cast<uint64_t>(host_id_) << 32) |
                           static_cast<uint64_t>(n);
          conn->Read(laddr, raddr, wr_id);

          // replace with raw polling

          if (conn->ProcessCompletions(1) != 1) {
            ROMULUS_DEBUG("Error polling in failure detector for machine {}.",
                          n);
            detected_[n] = true;
            continue;
          }
          uint64_t peer_heartbeat =
              *reinterpret_cast<uint64_t*>(laddr.addr + laddr.offset);
          peer_heartbeats[n] = peer_heartbeat;
          // ROMULUS_DEBUG(
          //     "[HEARTBEAT] Read peer {} heartbeat: {}, my counter: {}", n,
          //     peer_heartbeat, heartbeat_counter);
        }

        if (old_peer_heartbeats.empty()) {
          old_peer_heartbeats = peer_heartbeats;
          continue;
        }
        // interpret
        for (int n = 0; n < system_size_; ++n) {
          if (n == host_id_) continue;

          if (peer_heartbeats[n] == old_peer_heartbeats[n]) {
            scores[n]++;
            ROMULUS_DEBUG(
                "[HEARTBEAT] Peer {} heartbeat unchanged at {}. Score: {}",
                n, peer_heartbeats[n], scores[n]);
          } else {
            scores[n] = 0;
          }

          if (scores[n] > FAILURE_THRESHOLD && !detected_[n]) {
            detected_[n] = true;
            int leader_id = leader_.load().GetPromiseBallot() % system_size_;

            if (n == leader_id) {
              stable_leader_ = false;
              failover_detected_ = true;
              new_leader_id_ = (n + 1) % system_size_;
              // ClearLogs();
              failover_start_time_ = std::chrono::steady_clock::now();
              ROMULUS_INFO(
                  "[FAILURE DETECTOR] Leader {} is suspected to have failed. "
                  "New leader will be {}.",
                  n, new_leader_id_.load());
            } else {
              ROMULUS_INFO(
                  "[FAILURE DETECTOR] Machine {} is suspected to have failed.",
                  n);
            }
          }
        }
        old_peer_heartbeats = peer_heartbeats;
        std::this_thread::sleep_for(std::chrono::microseconds(500));
      }
    }).detach();
  }

  bool CasPaxos::BroadcastLeader(State* new_leader) {
    // uint32_t ok_count = 0;
    std::vector<bool> ok(system_size_);
    auto laddr = memblock_.GetAddrInfo(kScratchRegionId);
    auto* laddr_raw = reinterpret_cast<uint8_t*>(laddr.addr);

    // Write the new leader meta-data to everyone's slot, including my own
    for (int i = 0; i < system_size_; i++) {
      auto* c = contexts_[i];
      if (detected_[i]) {
        continue;
      }
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
      romulus::WorkRequest::BuildWrite(laddr, raddr, wr_id, &c->wr);
      ROMULUS_ASSERT(c->conn->Post(&c->wr, 1),
                     "Error posting in leader change.");
      ROMULUS_ASSERT(c->conn->PollBatch({wr_id}) == 1,
                     "Error polling in leader change.");
    }

    // while (ok_count < system_size_) {
    //   for (uint32_t i = 0; i < system_size_; ++i) {
    //     if (ok[i]) continue;
    //     auto* c = contexts_[i];
    //     ok[i] = PollCompletionsOnce(c->conn, wr_id_) > 0;
    //     if (ok[i]) ++ok_count;
    //   }
    // }
    return true;
  }

  State CasPaxos::ReadLeaderSlot() {
    // Landing space for the reads
    // we only need to read once from our own slot
    auto* c = contexts_[host_id_];
    auto laddr = memblock_.GetAddrInfo(kScratchRegionId);
    laddr.offset = host_id_ * kSlotSize;
    laddr.length = kSlotSize;

    auto raddr = remote_addrs_[host_id_][kLeaderRegionId];
    raddr.addr_info.offset = 0;
    raddr.addr_info.length = kSlotSize;
    // Should be loopback
    c->conn = remote_conns_[host_id_].front();

    romulus::WorkRequest::BuildRead(laddr, raddr, wr_id_, &c->wr);
    ROMULUS_ASSERT(c->conn->Post(&c->wr, 1), "Error reading in leader change.");
    // Only expecting a single completion
    while (!PollCompletionsOnce(c->conn, wr_id_));

    State new_leader = *reinterpret_cast<State*>(laddr.addr + laddr.offset);
    ROMULUS_DEBUG("[Leader READ] ID={}",
                  new_leader.GetPromiseBallot() % system_size_);
    return new_leader;
  }