#pragma once

#include <aparray.h>
#include <romulus/cfg.h>
#include <romulus/cli.h>
#include <romulus/common.h>
#include <romulus/connection_manager.h>
#include <romulus/device.h>
#include <romulus/romulus.h>
#include <state.h>

#include <cassert>
#include <memory>
#include <numeric>
#include <semaphore>
#include <immintrin.h>

#include "cfg.h"

#define BATCH_SIZE 16

#define kRingSize 2048

// Scratch 0: Main thread
// Scratch 1: ClearLogs()
// Scratch 2: Failure detector thread
// Scratch 3: Forward handler thread
// Scratch 4: Forward issuer thread
constexpr uint32_t NUM_SCRATCH_REGIONS = 5;
constexpr uint64_t kNumWarmupIters = 1000;
constexpr uint32_t kMaxPipeDepth = 16;

// Compile time configurations for testing different optimizations
// #define DYNO_NO_WRITEBUF
// #define DYNO_NO_SENDALL

namespace paxos_st {

// IDs used when registering RDMA connections.
const std::string kRegistryName = "CasPaxos";
const std::string kPdId = "PdId";
const std::string kBlockId = "LogBlock";
const std::string kPrepScratchRegionId = "PrepScratchRegion";
const std::string kScratchRegionId = "MainScratchRegion";

const std::string kFailureDetectorId = "FailureDetectorRegion";
const std::string kProposedRegionId = "ProposedRegion";
const std::string kLogRegionId = "LogRegion";
const std::string kBufRegionPrefix = "BufRegion";
const std::string kLeaderRegionId = "LeaderRegion";
const std::string kForwarderRegionId = "ForwarderRegion";
const std::string kHeartBeatRegionId = "HeartBeatRegion";

const std::string kLocalId = "StagingBlock";
const std::string kLocalWriteId = "LocalWriteRegion";
const std::string kLocalReadId = "LocalReadRegion";
const std::string kLocalCasId = "LocalCasRegion";

constexpr uint32_t kSlotSize = sizeof(State);
constexpr uint32_t kForwardRingSize = 16;
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
        prom_offset_(0),
        prep_offset_(0),
        is_leader_(false),
        stable_leader_(args->bget(romulus::STABLE_LEADER)),
        multi_paxos_opt_(args->bget(romulus::MULTIPAX_OPT)),
        hostname_(args->sget(romulus::HOSTNAME)),
        host_id_(args->uget(romulus::NODE_ID)),
        quorum_(romulus::GetQuorum(peers.size() + 1)),
        peers_(peers),
        key_range_(args->uget(romulus::KEY_RANGE)),
        detected_(system_size_),
        device_(std::make_shared<romulus::Device>(transport_flag)),
        buf_size_(args->uget(romulus::BUF_SIZE)),
        num_qps_(args->uget(romulus::NUM_QP)),
        num_shared_cq_(args->uget(romulus::NUM_SHARED_CQ)), pipe_depth_(args->uget(romulus::OUTSTANDING_REQS)) {
    expected_.resize(system_size_);
    done_.resize(system_size_);
    swap_.resize(system_size_);
    state_.resize(system_size_);
    cached_conns_.resize(system_size_);
    preprepare_conns_.resize(system_size_);
    cached_raddrs_.resize(system_size_);
    prepared_swap_.resize(capacity_);
    preprepare_expected_.resize(system_size_);
    preprepare_done_.resize(system_size_);

    if (multi_paxos_opt_)
      preparer_thread_ = std::thread(&CasPaxos::Preparer, this);
  }

  ~CasPaxos() {
    delete raw_;
    SyncNodes();
  }

  CasPaxos(CasPaxos&& other)
      : args_(std::move(other.args_)),
        system_size_(other.system_size_),
        capacity_(other.capacity_),
        raw_(other.raw_),
        leader_(other.leader_.load()),
        wr_id_(other.wr_id_),
        prom_offset_(other.prom_offset_.load()),
        prep_offset_(other.prep_offset_.load()),
        is_leader_(other.is_leader_),
        stable_leader_(other.stable_leader_.load()),
        multi_paxos_opt_(other.multi_paxos_opt_),
        hostname_(std::move(other.hostname_)),
        host_id_(other.host_id_),
        quorum_(other.quorum_),
        peers_(std::move(other.peers_)),
        remote_addrs_(std::move(other.remote_addrs_)),
        remote_conns_(std::move(other.remote_conns_)),
        contexts_(std::move(other.contexts_)),
        expected_(std::move(other.expected_)),
        swap_(std::move(other.swap_)),
        // state_(std::move(other.state_)),
        done_(std::move(other.done_)),
        // polled_(std::move(other.polled_)),
        // wr_ids_(std::move(other.wr_ids_)),
        key_range_(other.key_range_),
        detected_(std::move(other.detected_)),
        cached_conns_(std::move(other.cached_conns_)),
        cached_raddrs_(std::move(other.cached_raddrs_)),
        cached_laddr_(other.cached_laddr_),
        device_(other.device_),
        conn_manager_(std::move(other.conn_manager_)),
        registry_(std::move(other.registry_)),
        memblock_(std::move(other.memblock_)),
        buf_size_(other.buf_size_),
        num_qps_(other.num_qps_),
        num_shared_cq_(other.num_shared_cq_) {
    other.raw_ = nullptr;
    uint64_t scratch_len = system_size_ * kSlotSize;
    uint64_t proposal_len = capacity_ * kSlotSize;
    uint64_t log_len = capacity_ * kSlotSize;

    scratch_ = romulus::APArraySlice(raw_, 0, scratch_len);
    proposed_state_ = romulus::APArraySlice(
        raw_, scratch_len * (NUM_SCRATCH_REGIONS + 1),
        scratch_len * (NUM_SCRATCH_REGIONS + 1) + proposal_len);
    log_ = romulus::APArraySlice(
        raw_, scratch_len * (NUM_SCRATCH_REGIONS + 1) + proposal_len,
        scratch_len * (NUM_SCRATCH_REGIONS + 1) + proposal_len + log_len);
  }

  void Init(std::string_view dev_name, int dev_port,
            std::unique_ptr<romulus::ConnectionRegistry> registry,
            std::unordered_map<uint64_t, std::string> mach_map);

  void Reset() override;

  void Propose(uint32_t len, uint8_t* buf) override;

  void Preprepare() override;

  void CatchUp() override;

  void SyncNodes() override;

  void CleanUp() override;

  std::vector<std::thread> FailureDetector() override;

  void Failover(int node_id);

  void Warmup() override;

 private:
  bool BroadcastLeader(State* new_leader);

  void ReadLeaderSlot();

  void peer_heartbeat(int tid);

  void loopback_heartbeat();

  void ProposeInternal(Value& v);

  bool TryCatchUp();

  Ballot GlobalBallot();

  void ClearLogs();

  uint64_t ExtractId(State& st);

  bool Prepare() override;

  bool Promise(Value& v) override;

  void Preparer();

  std::atomic<bool>* isFailureDetected() override {
    return &failover_detected_;
  }

  bool isLeaderStable() override { return stable_leader_; }

  bool isLeader() override { return is_leader_; }

  int MaybeLeaderId() override { return new_leader_id_.load(); }

  // void ConditionalReset() override;

  uint32_t GetOffset() override { return prom_offset_.load(); }

  // Member variables.
 protected:
  // Global arg map
  std::shared_ptr<romulus::ArgMap> args_;

  // Total number of nodes in the system.
  uint8_t system_size_;

  // Number of slots in the log.
  const uint32_t capacity_;

  Ballot MakeBallot(uint32_t round);

  Ballot BumpBallot(Ballot observed_ballot);

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
  std::atomic<uint32_t> prom_offset_;
  std::atomic<uint32_t> prep_offset_;
  std::vector<State> prepared_swap_;

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

  // Reusable metadata between Prepare and Promise phases
  std::vector<State> expected_;
  std::vector<State> swap_;
  std::vector<State> state_;
  std::vector<bool> done_;

  std::vector<State> preprepare_expected_;
std::vector<bool> preprepare_done_;


  Ballot curr_promise_ballot_;

  // Metadata to indicate whether a CAS for a given acceptor has been POLLED
  // std::vector<bool> polled_;
  // std::vector<uint64_t> wr_ids_;

  uint32_t key_range_;

  std::binary_semaphore preprepare_sem_{0};
  std::atomic<bool> threads_running_ = true;
  std::thread preparer_thread_;

  // Metrics for a one-off failover test
  std::atomic<bool> failover_detected_ = false;
  std::chrono::steady_clock::time_point failover_start_time_;
  double failover_time_;
  std::atomic<int> new_leader_id_ = system_size_ - 1;

  std::vector<std::vector<double>> promise_bench_times_;
  std::vector<std::atomic<bool>> detected_;

  std::vector<romulus::ReliableConnection*> cached_conns_;
  std::vector<romulus::ReliableConnection*> preprepare_conns_;
  std::vector<romulus::RemoteAddr> cached_raddrs_;
  romulus::AddrInfo cached_laddr_;
  romulus::AddrInfo preprepare_laddr_;
  
  

  // RDMA related members.
  std::shared_ptr<romulus::Device> device_;
  std::unique_ptr<romulus::ConnectionManager> conn_manager_;
  std::unique_ptr<romulus::ConnectionRegistry> registry_;
  romulus::MemBlock memblock_;
  uint64_t buf_size_;
  uint64_t num_qps_;
  uint64_t num_shared_cq_;
  uint64_t pipe_depth_;
};

}  // namespace paxos_st