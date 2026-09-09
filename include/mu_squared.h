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

#include "cas_paxos.h"
#include "cfg.h"
#include "contexts.h"
#include "romulus/common.h"
#include "romulus/connection_manager.h"
#include "romulus/rc.h"
#include "romulus/registry.h"
#include "state.h"
#include "util.h"
#include "workload.h"

struct alignas(64) perm_req_t {
  uint64_t seq;
};

struct alignas(64) perm_grant_t {
  // [ 1 is_prev_owner | 1 cycled | 30 epoch | 32 fuo ]
  static constexpr uint64_t kOwnerBit = 1ULL << 63;
  static constexpr uint64_t kCycledBit = 1ULL << 62;
  static constexpr uint64_t kEpochShift = 32;
  static constexpr uint64_t kEpochMask = 0x3FFFFFFFULL
                                         << kEpochShift; // [61:32]
  static constexpr uint64_t kFuoMask = 0xFFFFFFFFULL;    // [31:0]

  uint64_t raw_;

  perm_grant_t() : raw_(0) {}
  explicit perm_grant_t(uint64_t raw) : raw_(raw) {}
  perm_grant_t(bool is_owner, bool cycled, uint64_t epoch, uint64_t fuo)
      : raw_((static_cast<uint64_t>(is_owner) << 63) |
             (static_cast<uint64_t>(cycled) << 62) |
             ((epoch << kEpochShift) & kEpochMask) | (fuo & kFuoMask)) {}

  bool IsPrevOwner() const { return raw_ & kOwnerBit; }
  bool Cycled() const { return raw_ & kCycledBit; }
  uint64_t Epoch() const { return (raw_ & kEpochMask) >> kEpochShift; }
  uint64_t FUO() const { return raw_ & kFuoMask; }

  void SetOwner(bool is_owner) {
    raw_ = (raw_ & ~kOwnerBit) | (static_cast<uint64_t>(is_owner) << 63);
  }
  void SetCycled(bool cycled) {
    raw_ = (raw_ & ~kCycledBit) | (static_cast<uint64_t>(cycled) << 62);
  }
  void SetEpoch(uint64_t epoch) {
    raw_ = (raw_ & ~kEpochMask) | ((epoch << kEpochShift) & kEpochMask);
  }
  void SetFUO(uint64_t fuo) { raw_ = (raw_ & ~kFuoMask) | (fuo & kFuoMask); }
};

struct LeaseEntry {
  uint64_t owner;
  uint64_t epoch;
  bool needs_fuo;
  bool operator==(const LeaseEntry &) const = default; // C++20
};

struct wr_id_t {
  uint64_t raw;

  wr_id_t(uint64_t id, uint64_t shard_id, uint64_t epoch) {
    raw = (id << 48) | (shard_id << 32) | epoch;
  }
  wr_id_t(uint64_t raw) : raw(raw) {}
  uint64_t GetID() const { return raw >> 48; }
  uint64_t GetShardID() const { return (raw >> 32) & 0xFFFF; }
  uint64_t GetEpoch() const { return raw & 0xFFFFFFFF; }
};

namespace mu_squared {

constexpr uint64_t kNoOwner = 0xD0E0A0D0;
constexpr uint64_t kPermNull = 0x0D0E0A0D;
constexpr int kFullPermission =
    romulus::PERM_FLAGS::LOCAL_READ | romulus::PERM_FLAGS::LOCAL_WRITE |
    romulus::PERM_FLAGS::REMOTE_READ | romulus::PERM_FLAGS::REMOTE_WRITE |
    romulus::PERM_FLAGS::REMOTE_ATOMIC;
constexpr int kNoPermission =
    romulus::PERM_FLAGS::LOCAL_READ | romulus::PERM_FLAGS::LOCAL_WRITE;
constexpr uint32_t kSlotSize = sizeof(State);
constexpr uint64_t kNumProposals = (1ULL << 12);
constexpr uint32_t kMaxStartingBackoff = 100; // us
constexpr uint32_t kMaxProposeDepth = 100;
constexpr uint32_t kPermTimeout_ms = 100;
const std::string kPdId = "PdId";
const std::string kBlockId = "LogBlock";
const std::string kScratchRegionId = "ScratchRegion";
const std::string kProposedRegionId = "ProposedRegion";
const std::string kFuoRegionId = "FuoRegion";
const std::string kFDLocalRegionId = "FailureDetectorLocalRegion";
const std::string kFDRemoteRegionId = "FailureDetectorRemoteRegion";
const std::string kPermHandlerScratchRegionId = "PermHanlderScratchRegion";
const std::string kPermRequesterScratchRegionId = "PermRequesterScratchRegion";
const std::string kPermReqRegionId = "PermReqRegion";
const std::string kPermGrantRegionId = "PermGrantRegion";
const std::string kLogRegionId = "LogRegion";

} // namespace mu_squared

struct PermCtx {
  std::vector<uint64_t> last_req;
  uint64_t current_owner = mu_squared::kNoOwner;
  uint64_t grant_epoch = 0;
  romulus::LocalAddr laddr;
  volatile uint64_t *req_raw = nullptr;
};

class MuSquared {
public:
  explicit MuSquared(std::shared_ptr<romulus::ArgMap> args,
                     uint64_t system_size,
                     std::shared_ptr<romulus::Device> device);
  ~MuSquared();

  void Init(std::string_view dev_name, int dev_port,
            std::unique_ptr<romulus::ConnectionRegistry> registry,
            std::unordered_map<uint64_t, std::string> mach_map);
  void SpawnThreads();
  void Propose(uint64_t target_shard, txn_t<int> &txn, uint32_t depth = 0);
  void Reset(uint64_t shard_id);
  void Cleanup();
  void Warmup();
  void Sync();
  std::vector<txn_t<int>> GetProposals();
  uint64_t SelectShard(int key);

private:
  void DrainCQ();
  void ResetLog(uint64_t shard_id);
  void HandleRequests(uint64_t shard_id, PermCtx &ctx);
  bool AcquirePermissions(uint64_t shard_id,
                          std::vector<std::pair<uint64_t, PermCtx>> &owned);
  bool PollPipeline(uint64_t shard_id, uint64_t target);
  bool FastCommit_Single(uint64_t target_shard, txn_t<int>& txn);
  bool FastCommit_Pipe(uint64_t target_shard, txn_t<int>& txn);
  void PermHandler(uint64_t tid);
  void FailureDetector();
  uint64_t Acquire_FUO();
  std::string GenLogID(uint64_t shard_id);
  uint64_t SelectHandler(uint64_t s) const;

  void Shutdown();

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
  std::vector<LeaseEntry> lease_cache_;
  uint64_t capacity_;
  uint64_t pipeline_depth_;
  std::atomic<uint64_t> req_epoch_;
  std::unique_ptr<std::atomic<uint64_t>[]> fuos_;
  bool need_fuo_scan_;

  std::vector<std::vector<uint64_t>> scoreboard_;
  std::vector<uint64_t> confirmed_;
  std::vector<uint64_t> seq_;

  // Workload
  std::vector<txn_t<int>> proposals_;

  // RDMA resources
  std::shared_ptr<romulus::Device> device_;
  std::unique_ptr<romulus::ConnectionManager> conn_manager_;
  std::unique_ptr<romulus::ConnectionRegistry> registry_;
  romulus::MemBlock memblock_;
  uint64_t num_shared_cq_;
  uint64_t num_qps_;

  // Permissions handling
  std::unique_ptr<std::atomic<bool>[]> want_perms_;
  std::unique_ptr<std::atomic<bool>[]> perm_acks_;

  std::unordered_map<uint64_t,
                     std::unordered_map<std::string, romulus::RemoteAddr>>
      remote_addrs_;
  std::unordered_map<uint64_t, std::vector<romulus::ReliableConnection *>>
      remote_conns_;
  cons_ctx_t cons_ctx_;               // shared cq
  replication_ctx_t replication_ctx_; // shared cq
  perm_ctx_t perm_handler_ctx_;       // shared cq
  fd_ctx_t fd_ctx_;                   // NO shared cq

  // Local view of remotely accessible memory.
  romulus::APArray<State, mu_squared::kSlotSize, CACHE_PREFETCH_SIZE> *raw_;
  romulus::APArraySlice<State, mu_squared::kSlotSize, CACHE_PREFETCH_SIZE>
      scratch_;
  romulus::APArraySlice<State, mu_squared::kSlotSize, CACHE_PREFETCH_SIZE>
      proposed_state_;
  romulus::APArraySlice<State, mu_squared::kSlotSize, CACHE_PREFETCH_SIZE>
      lease_table_;
  romulus::APArraySlice<State, mu_squared::kSlotSize, CACHE_PREFETCH_SIZE> log_;

  // Threads
  uint64_t num_handlers_;
  bool no_outliers_;
  std::vector<std::thread> perm_threads_;
  std::thread fd_thread_;
};
