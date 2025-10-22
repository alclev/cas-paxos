#pragma once

#include "common/aparray.h"
#include "common/common.h"
#include "connection_manager/connection_manager.h"
#include "device/device.h"
#include "paxos.h"

// Compile time configurations for testing different optimizations
// #define DYNO_NO_WRITEBUF
// #define DYNO_NO_SENDALL

namespace paxos_st {

// IDs used when registering RDMA connections.
const std::string kRegistryName = "CasPaxos";
const std::string kPdId = "PdId";
const std::string kBlockId = "LogBlock";
const std::string kScratchRegionId = "ScratchRegion";
const std::string kBallotRegionId = "BallotRegion";
const std::string kLogRegionId = "LogRegion";
const std::string kBufRegionPrefix = "BufRegion";

constexpr uint32_t kSlotSize = sizeof(State);
constexpr uint32_t kNumBufSlots = 50000000;
constexpr absl::Duration kTimeout = absl::Minutes(2);
constexpr uint16_t kNullBallot = std::numeric_limits<uint16_t>::min();
constexpr uint32_t kMaxStartingBackoff = 3600;

struct RemoteContext {
  State* scratch_state;
  State* proposed_state;

  dyno::ReliableConnection* conn;

  // Log related info
  dyno::AddrInfo scratch_laddr;
  dyno::RemoteAddr proposal_raddr;
  dyno::RemoteAddr log_raddr;

  // Buffer related info
  dyno::RemoteAddr buf_raddr;
  dyno::AddrInfo buf_laddr;

  dyno::WorkRequest buf_wr;
  dyno::WorkRequest log_wr;
  bool post_buf_wr;

  RemoteContext() {}
  RemoteContext(const RemoteContext& c)
      : scratch_state(c.scratch_state),
        proposed_state(c.proposed_state),
        conn(c.conn),
        scratch_laddr(c.scratch_laddr),
        proposal_raddr(c.proposal_raddr),
        log_raddr(c.log_raddr),
        buf_wr(c.buf_wr),
        log_wr(c.log_wr),
        post_buf_wr(c.post_buf_wr) {}
};

class CasPaxos : public Paxos {
 public:
  CasPaxos(uint32_t capacity, std::string hostname, uint8_t host_id,
           std::vector<std::string> peers);
  void Init(std::string_view dev_name, int dev_port,
            std::unique_ptr<dyno::ConnectionRegistry> registry, bool stable_leader = false);
  void Reset() override;
  void Propose(uint32_t len, uint8_t* buf) override;
  void CatchUp() override;
  void SyncNodes() override;
  void CleanUp() override;
  ~CasPaxos();

 private:
  uint32_t PerpareWrite(uint32_t len, uint8_t* buf);
  void ProposeInternal(Value v);
  bool TryCatchUp();
  void UpdateBallot();
  bool Prepare();
  bool Promise(Value v);
  void DumpLog();

  // Member variables.
 private:
  // Total number of nodes in the system.
  const uint8_t system_size_;

  // Number of slots in the log.
  const uint32_t capacity_;

  // Local view of remotely accessible memory.
  dyno::APArray<State, kSlotSize, CACHE_PREFETCH_SIZE>* raw_;
  dyno::APArraySlice<State, kSlotSize, CACHE_PREFETCH_SIZE> scratch_;
  dyno::APArraySlice<State, kSlotSize, CACHE_PREFETCH_SIZE> proposed_state_;
  dyno::APArraySlice<State, kSlotSize, CACHE_PREFETCH_SIZE> log_;

  uint8_t* buf_;
  uint32_t buf_offset_ = 0;
  uint64_t buf_chunk_size_;

  uint64_t wr_id_ = 0;
  uint32_t log_offset_ = 0;

  // Whether this node thinks its the leader.
  bool is_leader_;
  bool stable_leader_;

  // Hostname used during registration to connect with peers.
  std::string hostname_;

  // The node id of this node.
  uint8_t host_id_;

  // Number of acknowledgements required to reach a quorum.
  const uint8_t quorum_;

  // Names of remote peers.
  std::vector<std::string> peers_;

  // Stores a list of worker contexts.
  std::vector<RemoteContext*> contexts_;

  // RDMA related members.
  dyno::Device device_;
  std::unique_ptr<dyno::ConnectionManager> conn_manager_;
  std::unique_ptr<dyno::ConnectionRegistry> registry_;
  dyno::MemBlock memblock_;
};

}  // namespace paxos_st