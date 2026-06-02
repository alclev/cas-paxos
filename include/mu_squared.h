#pragma once

#include <atomic>
#include <barrier>
#include <cstdint>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "cas_paxos_st.h"
#include "mpsc_queue.h"
#include "workload.h"

constexpr uint64_t kQueueSize = 1024;
constexpr uint64_t kDrainFrequency = 5;

struct OP {
  uint32_t len;
  uint32_t raw;
  uint32_t fuo;
  std::atomic<int>* acks;
};

namespace paxos_st {

class MuSquared : public CasPaxos {
 public:
  explicit MuSquared(CasPaxos&& paxos,
                     std::vector<std::pair<uint32_t, uint8_t*>> proposals);

  void LeasePropose(uint32_t len, uint8_t* buf, bool is_lease = false);
  void Cleanup();
  void Warmup() override;
  uint64_t GetTotalOps();
  std::vector<double> AggregateLatencies();
  void StartCommitThreads();

 private:
  // Paxos phases
  bool LeasePrepare(uint32_t offset);
  bool LeasePromise(uint32_t offset, Value& v);

  // Thread entry points
  void FastCommit(uint32_t offset);
  void Poller();
  void Forwarder();
  void ForwardHandler();

  // RDMA utility
  void DrainCQ();

  std::vector<std::pair<uint32_t, uint8_t*>> proposals_;

  // Lease / sharding
  uint32_t shard_size_;
  std::vector<uint32_t> lease_boundaries_;
  std::atomic<bool> lease_established_;

  // Threads/Synchronization
  std::atomic<bool> threads_alive_;
  std::mutex proposals_mutex_;
  std::thread poller_;

  // Measurements
  uint64_t op_counter_;
  std::vector<std::vector<double>> per_thread_latencies_;

  // Queues
  std::vector<std::unique_ptr<MPSCQueue<OP, kQueueSize>>> commit_queues_;
  std::unique_ptr<MPSCQueue<OP, kQueueSize>> forward_queue_;

  // Forwarding
  std::thread forwarder_thread_;
  std::thread forward_handler_thread_;

  // RDMA resources
  std::vector<romulus::ReliableConnection*> forwarding_conns_;
  std::atomic<uint64_t> fuo_;
  alignas(64) romulus::WorkRequest cached_write_;
  alignas(64) romulus::WorkRequest cached_cas_;
  alignas(64) std::atomic<int> ack_;
};

}  // namespace paxos_st