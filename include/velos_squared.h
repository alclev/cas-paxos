#pragma once

#include <cstdint>
#include <memory>
#include <unordered_map>

#include "romulus/common.h"
#include "romulus/connection_manager.h"
#include "romulus/rc.h"
#include "romulus/registry.h"

#include "aparray.h"
#include "state.h"
#include "util.h"
#include "workload.h"
#include "queue.h"

struct alignas(64) prep_req_t {
  uint64_t shard_id;
  uint64_t wr_id;
  State state;
};

namespace velos_squared {

constexpr uint32_t kSlotSize = sizeof(State);
constexpr uint64_t kNumProposals = (1ULL << 12);
constexpr uint32_t kMaxStartingBackoff = 100; // us
constexpr uint32_t kMaxProposeDepth = 100;
constexpr uint32_t kPermTimeout_ms = 100;
constexpr uint32_t kQueueSize = 64;
const std::string kFDLocalRegionId = "FailureDetectorLocalRegion";
const std::string kFDRemoteRegionId = "FailureDetectorRemoteRegion";

const std::string kPdId = "PdId";
const std::string kBlockId = "LogBlock";
const std::string kScratchRegionId = "ScratchRegion";
const std::string kProposedRegionId = "ProposedRegion";
const std::string kLogRegionId = "LogRegion";
const std::string kPreScratchRegionId = "PrepareStaging";

} // namespace velos_squared

class VelosSquared {
public:
  explicit VelosSquared(std::shared_ptr<romulus::ArgMap> args,
                        uint64_t system_size,
                        std::shared_ptr<romulus::Device> device);
  ~VelosSquared();

  void Init(std::string_view dev_name, int dev_port,
            std::unique_ptr<romulus::ConnectionRegistry> registry,
            std::unordered_map<uint64_t, std::string> mach_map);

  std::vector<txn_t<int>> GetProposals();
  uint64_t SelectShard(int key);

  void Propose(uint64_t target_shard, Value &v, uint32_t depth = 0);
  bool Prepare(uint64_t target_shard);
  void PrepareHandler();
  void FailureDetector();
  bool Promise_Single(uint64_t target_shard, Value &v);
  bool Promise_Pipe(uint64_t target_shard, Value &v);

  std::string GenLogID(uint64_t shard_id);
  void SpawnThreads();
  void Shutdown();
  void Reset(uint64_t shard_id);
  void Sync();
  void Warmup();

private:
  void ResetLog(uint64_t shard_id);
  void DrainCQ(ibv_cq *cq_raw);
  Ballot MakeBallot(uint32_t round);

  // General
  std::shared_ptr<romulus::ArgMap> args_;
  uint64_t id_;
  std::string hostname_;
  uint64_t system_size_;
  uint64_t quorum_;
  uint64_t num_shards_;
  uint64_t shard_size_;
  uint64_t capacity_;
  uint64_t pipeline_depth_;

  // Workload
  std::vector<txn_t<int>> proposals_;
  bool no_outliers_;

  // Consensus
  std::unique_ptr<std::atomic<uint64_t>[]> prep_offsets_;
  std::unique_ptr<std::atomic<uint64_t>[]> prom_offsets_;
  // shard,node_lst
  std::vector<std::vector<State>> preprepare_expected_;
  std::vector<std::vector<bool>> preprepare_done_;
  Ballot local_ballot_;

  std::unique_ptr<std::atomic<uint64_t>[]> fuos_;
  ReaderWriterQueue<prep_req_t, velos_squared::kQueueSize> prep_queue_;
  std::vector<uint64_t> my_shards_;
  std::vector<uint64_t> wr_ids_;

  // RDMA resources
  std::shared_ptr<romulus::Device> device_;
  std::unique_ptr<romulus::ConnectionManager> conn_manager_;
  std::unique_ptr<romulus::ConnectionRegistry> registry_;
  romulus::MemBlock memblock_;
  uint64_t num_shared_cq_;
  uint64_t num_qps_;

  std::unordered_map<uint64_t,
                     std::unordered_map<std::string, romulus::RemoteAddr>>
    remote_addrs_;
  std::unordered_map<uint64_t, std::vector<romulus::ReliableConnection *>>
    remote_conns_;

  // Local view of remotely accessible memory.
  romulus::APArray<State, velos_squared::kSlotSize, CACHE_PREFETCH_SIZE> *raw_;
  romulus::APArraySlice<State, velos_squared::kSlotSize, CACHE_PREFETCH_SIZE>
    scratch_;
  romulus::APArraySlice<State, velos_squared::kSlotSize, CACHE_PREFETCH_SIZE>
    proposed_state_;
  romulus::APArraySlice<State, velos_squared::kSlotSize, CACHE_PREFETCH_SIZE>
    log_;

  // Threads
  std::thread fd_thread_;
  std::thread prepare_th_;
  std::atomic<bool> failure_detector_running_;
  std::atomic<bool> prepare_running_;
};