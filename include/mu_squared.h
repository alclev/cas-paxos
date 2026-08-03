#pragma once

#include <aparray.h>

#include <atomic>
#include <barrier>
#include <cstdint>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "cfg.h"
#include "lease_remote.h"
#include "romulus/common.h"
#include "romulus/connection_manager.h"
#include "romulus/rc.h"
#include "romulus/registry.h"
#include "state.h"
#include "util.h"
#include "workload.h"

struct wr_id_t {
  uint64_t raw;

  wr_id_t(uint64_t id, uint64_t shard_id, uint64_t epoch) {
    raw= (id << 48) | (shard_id << 32) | epoch;
  }
  wr_id_t(uint64_t raw) : raw(raw) {}
  uint64_t GetID() const { return raw >> 48; }
  uint64_t GetShardID() const { return (raw >> 32) & 0xFFFF; }
  uint64_t GetEpoch() const { return raw & 0xFFFFFFFF; }
};


struct RawMetrics {
  uint64_t total_ops;
  std::vector<double> latencies;
};

namespace mu_squared {

constexpr uint32_t kSlotSize = sizeof(State);
constexpr uint64_t kNumProposals = (1ULL << 20);
constexpr uint32_t kMaxStartingBackoff = 100;  // us
const std::string kPdId = "PdId";
const std::string kBlockId = "LogBlock";
const std::string kScratchRegionId = "ScratchRegion";
const std::string kProposedRegionId = "ProposedRegion";
const std::string kLeaseRegionId = "LeaseRegion";
const std::string kLogRegionId = "LogRegion";

}  // namespace mu_squared

class MuSquared {
 public:
  explicit MuSquared(std::shared_ptr<romulus::ArgMap> args,
                     uint64_t system_size,
                     std::shared_ptr<romulus::Device> device);
  void RemoteDump();

  void Init(std::string_view dev_name, int dev_port,
            std::unique_ptr<romulus::ConnectionRegistry> registry,
            std::unordered_map<uint64_t, std::string> mach_map);
  void Propose(txn_t<int>& txn);
  void Cleanup();
  void Warmup();
  void Sync();
  RawMetrics GetStats();
  std::vector<txn_t<int>> GetProposals();

 private:
  void DrainCQ();
  bool AcquireLease(uint64_t shard_id);
  bool Prepare(uint32_t offset);
  bool Promise(uint32_t offset, Value v);
  void FastCommit(uint64_t shard_id, txn_t<int>& txn);
  uint64_t SelectShard(int key);
  Ballot MakeBallot(uint32_t round);
  Ballot BumpBallot(Ballot observed_ballot);
  std::string GenLogID(uint64_t shard_id);
  // General
  std::shared_ptr<romulus::ArgMap> args_;
  uint64_t id_;
  std::string hostname_;
  uint64_t system_size_;
  uint64_t quorum_;

  // Shard
  uint64_t num_shards_;
  std::vector<std::pair<uint64_t, uint64_t>> shard_ranges_;
  std::vector<uint64_t> my_shards_;
  uint64_t shard_size_;

  // Consensus
  std::vector<uint64_t> fuos_;
  uint64_t capacity_;
  uint64_t pipeline_depth_;
  Ballot local_ballot_;
  std::vector<State> expected_;
  std::unordered_set<bool> needs_fuo_scan_;

  // Workload
  std::vector<txn_t<int>> proposals_;

  // RDMA resources
  std::unordered_map<uint64_t, std::vector<romulus::ReliableConnection*>>
      remote_conns_;
  std::unordered_map<uint64_t,
                     std::unordered_map<std::string, romulus::RemoteAddr>>
      remote_addrs_;
  std::shared_ptr<romulus::Device> device_;
  std::unique_ptr<romulus::ConnectionManager> conn_manager_;
  std::unique_ptr<romulus::ConnectionRegistry> registry_;
  romulus::MemBlock memblock_;
  uint64_t num_shared_cq_;
  uint64_t num_qps_;
  std::vector<romulus::ReliableConnection*> cached_conns_;
  std::vector<romulus::RemoteAddr> cached_raddrs_;
  romulus::AddrInfo cached_laddr_;

  // Local view of remotely accessible memory.
  romulus::APArray<State, mu_squared::kSlotSize, CACHE_PREFETCH_SIZE>* raw_;
  romulus::APArraySlice<State, mu_squared::kSlotSize, CACHE_PREFETCH_SIZE>
      scratch_;
  romulus::APArraySlice<State, mu_squared::kSlotSize, CACHE_PREFETCH_SIZE>
      proposed_state_;
  romulus::APArraySlice<State, mu_squared::kSlotSize, CACHE_PREFETCH_SIZE>
      lease_table_;
  romulus::APArraySlice<State, mu_squared::kSlotSize, CACHE_PREFETCH_SIZE> log_;

  // Measurements
  RawMetrics metrics_;
};
