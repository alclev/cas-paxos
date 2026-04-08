#pragma once

#include <aparray.h>
#include <romulus/common.h>
#include <romulus/connection_manager.h>
#include <romulus/device.h>
#include <romulus/romulus.h>
#include <romulus/cli.h>
#include <romulus/cfg.h>
#include <state.h>

#include <cassert>
#include <memory>
#include <numeric>

#include "cfg.h"


#define BATCH_SIZE 16

#define kRingSize 1024

#define FAILURE_THRESHOLD 100
#define WR_FAILIRE_THRESHOLD 10

// Compile time configurations for testing different optimizations
// #define DYNO_NO_WRITEBUF
// #define DYNO_NO_SENDALL

namespace paxos_st {

// IDs used when registering RDMA connections.
const std::string kRegistryName = "CasPaxos";
const std::string kPdId = "PdId";
const std::string kBlockId = "LogBlock";
const std::string kScratchRegionId = "ScratchRegion";
const std::string kScratchExtraRegionId = "ScratchExtraRegion";
const std::string kProposedRegionId = "ProposedRegion";
const std::string kLogRegionId = "LogRegion";
const std::string kBufRegionPrefix = "BufRegion";
const std::string kLeaderRegionId = "LeaderRegion";
const std::string kHeartBeatRegionId = "HeartBeatRegion";

const std::string kLocalId = "StagingBlock";
const std::string kLocalWriteId = "LocalWriteRegion";
const std::string kLocalReadId = "LocalReadRegion";
const std::string kLocalCasId = "LocalCasRegion";

constexpr uint32_t kSlotSize = sizeof(State);
constexpr uint32_t kNumBufSlots = 50000000;
constexpr uint16_t kNullBallot = 0;
constexpr uint32_t kNullValue = std::numeric_limits<uint32_t>::max();
constexpr uint32_t kShutdown = std::numeric_limits<uint32_t>::max() - 1;
constexpr uint32_t kMaxStartingBackoff = 3600;

struct RemoteContext {
  State* scratch_state;
  State* proposed_state;

  romulus::ReliableConnection* conn;

  // Log related info
  romulus::AddrInfo scratch_laddr;
  romulus::RemoteAddr proposal_raddr;
  romulus::RemoteAddr log_raddr;

  // Head of WR chain
  romulus::WorkRequest wr;
  RemoteContext() {}
  RemoteContext(const RemoteContext& c)
      : scratch_state(c.scratch_state),
        proposed_state(c.proposed_state),
        conn(c.conn),
        scratch_laddr(c.scratch_laddr),
        proposal_raddr(c.proposal_raddr),
        log_raddr(c.log_raddr),
        wr(c.wr) {}
  std::string ToString() const {
    std::ostringstream oss;
    oss << "RemoteContext{"
        << "scratch_state=" << static_cast<const void*>(scratch_state) << ", "
        << "proposed_state=" << static_cast<const void*>(proposed_state) << ", "
        << "conn=" << static_cast<const void*>(conn) << ", "
        << "scratch_laddr={addr=" << reinterpret_cast<void*>(scratch_laddr.addr)
        << ", offset=" << scratch_laddr.offset
        << ", length=" << scratch_laddr.length << "}, "
        << "proposal_raddr={addr="
        << reinterpret_cast<void*>(proposal_raddr.addr_info.addr)
        << ", offset=" << proposal_raddr.addr_info.offset
        << ", length=" << proposal_raddr.addr_info.length << "}, "
        << "log_raddr={addr="
        << reinterpret_cast<void*>(log_raddr.addr_info.addr)
        << ", offset=" << log_raddr.addr_info.offset
        << ", length=" << log_raddr.addr_info.length << "}"
        << "}";
    return oss.str();
  }
};

class CasPaxos : public Paxos {
 public:
  CasPaxos(std::shared_ptr<romulus::ArgMap> args,
           std::vector<std::string> peers, uint8_t transport_flag)
      : args_(args),
        system_size_(peers.size() + 1),
        capacity_(args->uget(romulus::CAPACITY)),
        wr_id_(0),
        log_offset_(0),
        is_leader_(false),
        stable_leader_(args->bget(romulus::STABLE_LEADER)),
        multi_paxos_opt_(args->bget(romulus::MULTIPAX_OPT)),
        hostname_(args->sget(romulus::HOSTNAME)),
        host_id_(args->uget(romulus::NODE_ID)),
        quorum_(romulus::GetQuorum(peers.size() + 1)),
        peers_(peers),
        detected_(system_size_),
        device_(transport_flag),
        buf_size_(args->uget(romulus::BUF_SIZE)),
        num_qps_(args->uget(romulus::NUM_QP)) {
    expected_.resize(system_size_);
    done_.resize(system_size_);
    polled_.resize(system_size_);
    wr_ids_.resize(system_size_);
  }

  ~CasPaxos() {
    delete raw_;
    SyncNodes();
  }

  void Init(std::string_view dev_name, int dev_port,
            std::unique_ptr<romulus::ConnectionRegistry> registry,
            std::unordered_map<uint64_t, std::string> mach_map);

  void Reset() override;

  void Propose([[maybe_unused]] uint32_t len,
               [[maybe_unused]] uint8_t* buf) override;

  void CatchUp() override;

  void SyncNodes() override;

  void CleanUp() override;

  void FailureDetector() override;

 private:
  bool BroadcastLeader(State* new_leader);

  State ReadLeaderSlot();

  void ProposeInternal(Value& v);

  bool TryCatchUp();

  Ballot GlobalBallot(); 

  Ballot BumpBallot(Ballot observed_ballot);

  Ballot MakeBallot(uint32_t round);

  void ClearLogs();

  State* Prepare() override;

  bool Promise(Value v) override;

  bool isLeaderStable() override { return stable_leader_; }

  bool isLeader() override { return is_leader_; }

  void ConditionalReset() override;

  uint32_t GetOffset() override { return log_offset_; }

  // Member variables.
 private:
  // Global arg map
  std::shared_ptr<romulus::ArgMap> args_;

  // Total number of nodes in the system.
  uint8_t system_size_;

  // Number of slots in the log.
  const uint32_t capacity_;

  // Local view of remotely accessible memory.
  romulus::APArray<State, kSlotSize, CACHE_PREFETCH_SIZE>* raw_;
  romulus::APArraySlice<State, kSlotSize, CACHE_PREFETCH_SIZE> scratch_;
  romulus::APArraySlice<State, kSlotSize, CACHE_PREFETCH_SIZE> proposed_state_;
  romulus::APArraySlice<State, kSlotSize, CACHE_PREFETCH_SIZE> log_;

  std::atomic<State> leader_;

  // Buffer fields for commit
  [[maybe_unused]] uint8_t* buf_;
  [[maybe_unused]] uint32_t buf_offset_ = 0;
  [[maybe_unused]] uint64_t buf_chunk_size_;

  uint64_t wr_id_ = 0;
  uint32_t log_offset_ = 0;

  // Whether this node thinks its the leader.
  bool is_leader_;
  // Whether there exists a stable leader
  std::atomic<bool> stable_leader_;
  // True if multi-paxos optimization has been toggled
  bool multi_paxos_opt_;

  // Hostname used during registration to connect with peers.
  std::string hostname_;

  // The node id of this node.
  uint8_t host_id_;

  // Number of acknowledgements required to reach a quorum.
  const uint8_t quorum_;

  // Names of remote peers.
  std::vector<std::string> peers_;

  std::unordered_map<uint64_t,
                     std::unordered_map<std::string, romulus::RemoteAddr>>
      remote_addrs_;
  std::unordered_map<uint64_t, std::vector<romulus::ReliableConnection*>>
      remote_conns_;

  std::vector<RemoteContext*> contexts_;

  Ballot local_ballot_ = 0;

  std::vector<State> expected_;
  // Metadata to indicate to indicate a SUCCESSFUL cas for the given
  // acceptor
  std::vector<bool> done_;
  // Metadata to indicate whether a CAS for a given acceptor has been POLLED
  std::vector<bool> polled_;
  std::vector<uint64_t> wr_ids_;

  std::atomic<bool> running_ = true;

  // Metrics for a one-off failover test
  std::atomic<bool> failover_detected_ = false;
  std::chrono::steady_clock::time_point failover_start_time_;
  double failover_time_;
  std::atomic<int> new_leader_id_ = system_size_ - 1;

  std::vector<std::vector<double>> promise_bench_times_;
  std::vector<std::atomic<bool>> detected_;

  // RDMA related members.
  romulus::Device device_;
  std::unique_ptr<romulus::ConnectionManager> conn_manager_;
  std::unique_ptr<romulus::ConnectionRegistry> registry_;
  romulus::MemBlock memblock_;
  uint64_t buf_size_;
  uint64_t num_qps_;
};

}  // namespace paxos_st