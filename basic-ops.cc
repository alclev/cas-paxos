#include <chrono>
#include <random>
#include <string>

#include "romulus/cfg.h"
#include "romulus/common.h"
#include "romulus/connection_manager.h"
#include "romulus/device.h"
#include "romulus/memblock.h"
#include "romulus/stats.h"
#include "romulus/util.h"

constexpr int kNumIters = static_cast<int>(1e4);
constexpr int EXP_MAX = 15;
const std::string outfile = "stats.csv";

// Output format:
// Multiples of 6 
// Payload [0] {
//   - uncontended write
//   - uncontended read
//   - uncontended cas
//   - contended write
//   - contended read
//   - contended cas
// }

int main(int argc, char* argv[]) {
  romulus::INIT();
  auto args = std::make_shared<romulus::ArgMap>();
  args->import(romulus::ARGS);
  args->parse(argc, argv);

  // Command line arguments
  int id = args->uget(romulus::NODE_ID);
  std::string registry_ip = args->sget(romulus::REGISTRY_IP);

  const std::string remote_str = args->sget(romulus::REMOTES);
  std::stringstream ss(remote_str);
  std::string remote;
  std::vector<std::string> machines;

  while (std::getline(ss, remote, ',')) {
    machines.push_back(remote);
  }
  // Ingest all the machines from the command line
  std::string host = machines.at(id);
  uint64_t system_size = machines.size();
  ROMULUS_INFO("Node {} of {} is {}", id, system_size, host);
  std::vector<std::string> remotes = machines;
  // Remotes does **not** include this node
  remotes.erase(remotes.begin() + id);
  remote = remotes.front();

  if (std::filesystem::exists(outfile)) std::filesystem::remove(outfile);

  // Create a registry to be used for this test, all keys are prefixed with
  // 'SimpleTest'. This registry is used for registering connections and
  // barrier.
  ROMULUS_DEBUG("Setting up registry");
  romulus::ConnectionRegistry registry("SimpleTest", registry_ip);

  auto work = [&](uint64_t kChunkSize) {
    // The device is responsible for setting up protection domains, which are
    // associated with chunks of remotely accessible memory.
    ROMULUS_DEBUG("Setting up device and allocating protection domain");
    const std::string kPdName = "SimpleTestPd";
    romulus::Device dev;
    dev.Open();
    dev.AllocatePd(kPdName);
    struct ibv_pd* pd = dev.GetPd(kPdName);

    // A memory block is a region of remotely accessible memory that is
    // associated with a single connection (i.e., QP). There can be several
    // sub-regions of the memory block, which are known as memory regions.
    const int kBlockSize = kChunkSize * system_size;
    const std::string kBlockId = "SimpleTestMemBlock";
    const std::string kMemRegionId = "SimpleTestMemRegion";
    ROMULUS_INFO("Allocating MemBlock {} of size {}", kBlockId, kBlockSize);
    uint8_t* raw = new uint8_t[kBlockSize];
    romulus::MemBlock memblock(kBlockId, pd, raw, kBlockSize);
    ROMULUS_INFO("Registering MemRegion {} of size {}", kMemRegionId,
                 kBlockSize);
    memblock.RegisterMemRegion(kMemRegionId, 0, kBlockSize);

    // A connection manager keeps track of all reliable connections (i.e., QPs)
    // that are connected to each memblock.
    ROMULUS_INFO("Setting up connection manager for node {}", id);
    romulus::ConnectionManager mgr(host, &registry, id, system_size);

    // The manager first registers the MemBlocks and indicates which nodes
    // (i.e., `remote`) can access them. Since this is a simple test, we only
    // register a single MemBlock.
    ROMULUS_DEBUG("Registering connections...");
    ROMULUS_ASSERT(mgr.Register(remotes, memblock, dev),
                   "Connection registration failed");
    // NB: We wait here to ensure that the barrier key has been written
    //     There is a **possible** race condition here
    sleep(3);
    // Barrier
    mgr.arrive_strict_barrier();
    // And then we connect. In order to have a connection, there must be a peer
    // MemBlock registered on a remote machine with the same Id.
    ROMULUS_INFO("Connecting...");
    ROMULUS_ASSERT(mgr.Connect(remotes, memblock), "Failed to connect");
    // Barrier again after we connect
    mgr.arrive_strict_barrier();
    // Create an unconnected MemBlock that is used to transfer data to and from
    // the NIC. Divide it into two portions, a write region and read region. The
    // former is used to define the bytes to write over the network, the latter
    // is used to store incoming reads.
    const std::string kLocalMemBlockId = "StagingBlock";
    const std::string kLocalWriteRegionId = "WriteStagingRegion";
    const std::string kLocalReadRegionId = "ReadStagingRegion";
    const std::string kLocalCASRegionId = "CASStagingRegion";
    uint8_t* local_raw = new uint8_t[kChunkSize * 3];
    romulus::MemBlock local_memblock(kLocalMemBlockId, pd, local_raw,
                                     kChunkSize * 3);
    local_memblock.RegisterMemRegion(kLocalWriteRegionId, 0, kChunkSize);
    local_memblock.RegisterMemRegion(kLocalReadRegionId, kChunkSize,
                                     kChunkSize);
    local_memblock.RegisterMemRegion(kLocalCASRegionId, kChunkSize * 2,
                                     kChunkSize);

    auto write_from_addr = local_memblock.GetAddrInfo(kLocalWriteRegionId);

    mgr.arrive_strict_barrier();

    // Before launching operations on all the connections, we cache the addrs
    // and connections
    std::unordered_map<std::string, romulus::RemoteAddr> remote_addrs;
    std::unordered_map<std::string, romulus::ReliableConnection*> remote_conns;
    romulus::RemoteAddr remote_addr;
    for (auto& remote : remotes) {
      mgr.GetRemoteAddr(remote, kBlockId, kMemRegionId, &remote_addr);
      remote_addrs.emplace(remote, remote_addr);
      remote_conns.emplace(remote, mgr.GetConnection(remote, kBlockId));
    }

    mgr.arrive_strict_barrier();
    // We start to perform operations at this point

    // ------------------- Uncontended Write -------------------
    std::unique_ptr<stats::collector_t> w_collector =
        std::make_unique<stats::collector_t>();
    w_collector->op_type = "Uncontended Write";
    auto write_start = std::chrono::steady_clock::now();
    for (auto& remote : remotes) {
      auto remote_conn = remote_conns[remote];
      auto remote_addr = remote_addrs[remote];
      // Execute write over the connection to the remote node
      remote_addr.addr_info.offset = id * kChunkSize;
      remote_addr.addr_info.length = kChunkSize;
      std::memset((uint8_t*)write_from_addr.addr, id, kChunkSize);
      for (int wr_id = 0; wr_id < kNumIters; wr_id++) {
        w_collector->ops++;
        w_collector->bytes += kChunkSize;
        auto now = std::chrono::steady_clock::now();
        remote_conn->Write(write_from_addr, remote_addr, wr_id);
        ROMULUS_ASSERT(remote_conn->ProcessCompletions(1) == 1,
                       "Failed to process completion");
        w_collector->times.push_back(std::chrono::duration<double, std::micro>(
                                         std::chrono::steady_clock::now() - now)
                                         .count());
      }
      w_collector->total_time_s =
          std::chrono::duration<double>(std::chrono::steady_clock::now() -
                                        write_start)
              .count();
    }
    auto w_stats = stats::digest(w_collector.get());
    w_stats.log_csv(outfile);
    mgr.arrive_strict_barrier();

    // ------------------- Uncontended READ -------------------
    auto read_to_addr = local_memblock.GetAddrInfo(kLocalReadRegionId);
    std::unique_ptr<stats::collector_t> r_collector =
        std::make_unique<stats::collector_t>();
    r_collector->op_type = "Uncontended Read";
    auto read_start = std::chrono::steady_clock::now();
    for (auto& remote : remotes) {
      auto remote_conn = remote_conns[remote];
      auto remote_addr = remote_addrs[remote];
      // Execute write over the connection to the remote node
      remote_addr.addr_info.offset = id * kChunkSize;
      remote_addr.addr_info.length = kChunkSize;
      for (int wr_id = 0; wr_id < kNumIters; wr_id++) {
        r_collector->ops++;
        r_collector->bytes += kChunkSize * 2;
        auto now = std::chrono::steady_clock::now();
        remote_conn->Read(read_to_addr, remote_addr, wr_id);
        ROMULUS_ASSERT(remote_conn->ProcessCompletions(1) == 1,
                       "Failed to process completion");
        r_collector->times.push_back(std::chrono::duration<double, std::micro>(
                                         std::chrono::steady_clock::now() - now)
                                         .count());
      }
      r_collector->total_time_s =
          std::chrono::duration<double>(std::chrono::steady_clock::now() -
                                        read_start)
              .count();
    }
    auto r_stats_v2 = stats::digest(r_collector.get());
    r_stats_v2.log_csv(outfile);
    mgr.arrive_strict_barrier();

    // ------------------- Uncontended CAS -------------------
    auto cas_to_addr = local_memblock.GetAddrInfo(kLocalCASRegionId);
    std::unique_ptr<stats::collector_t> c_collector =
        std::make_unique<stats::collector_t>();
    c_collector->op_type = "Uncontended CAS";
    cas_to_addr.length = sizeof(uint64_t);
    auto cas_start = std::chrono::steady_clock::now();
    for (auto& remote : remotes) {
      auto remote_conn = remote_conns[remote];
      auto remote_addr = remote_addrs[remote];
      // Execute write over the connection to the remote node
      remote_addr.addr_info.offset = id * kChunkSize;
      remote_addr.addr_info.length = sizeof(uint64_t);
      for (int wr_id = 0; wr_id < kNumIters; wr_id++) {
        c_collector->ops++;
        c_collector->bytes += sizeof(uint64_t);
        auto now = std::chrono::steady_clock::now();
        remote_conn->CompareAndSwap(cas_to_addr, remote_addr, 0, 1, wr_id);
        ROMULUS_ASSERT(remote_conn->ProcessCompletions(1) == 1,
                       "Failed to process completion");
        c_collector->times.push_back(std::chrono::duration<double, std::micro>(
                                         std::chrono::steady_clock::now() - now)
                                         .count());
      }
      c_collector->total_time_s =
          std::chrono::duration<double>(std::chrono::steady_clock::now() -
                                        cas_start)
              .count();
    }
    auto c_stats = stats::digest(c_collector.get());
    c_stats.log_csv(outfile);
    mgr.arrive_strict_barrier();

    // ------------------- Contended Write -------------------
    std::unique_ptr<stats::collector_t> w_collector_v2 =
        std::make_unique<stats::collector_t>();
    w_collector_v2->op_type = "Contended Write";
    auto write_start_v2 = std::chrono::steady_clock::now();
    for (auto& remote : remotes) {
      auto remote_conn = remote_conns[remote];
      auto remote_addr = remote_addrs[remote];
      // Execute write over the connection to the remote node
      remote_addr.addr_info.offset = 0;  // Hardcoding zero offset for contended
      remote_addr.addr_info.length = kChunkSize;
      std::memset((uint8_t*)write_from_addr.addr, id, kChunkSize);
      for (int wr_id = 0; wr_id < kNumIters; wr_id++) {
        w_collector_v2->ops++;
        w_collector_v2->bytes += kChunkSize;
        auto now = std::chrono::steady_clock::now();
        remote_conn->Write(write_from_addr, remote_addr, wr_id);
        ROMULUS_ASSERT(remote_conn->ProcessCompletions(1) == 1,
                       "Failed to process completion");
        w_collector_v2->times.push_back(
            std::chrono::duration<double, std::micro>(
                std::chrono::steady_clock::now() - now)
                .count());
      }
      w_collector_v2->total_time_s =
          std::chrono::duration<double>(std::chrono::steady_clock::now() -
                                        write_start_v2)
              .count();
    }
    auto w_stats_v2 = stats::digest(w_collector_v2.get());
    w_stats_v2.log_csv(outfile);
    mgr.arrive_strict_barrier();

    // ------------------- Contended READ -------------------
    std::unique_ptr<stats::collector_t> r_collector_v2 =
        std::make_unique<stats::collector_t>();
    r_collector_v2->op_type = "Contended Read";
    auto read_start_v2 = std::chrono::steady_clock::now();
    for (auto& remote : remotes) {
      auto remote_conn = remote_conns[remote];
      auto remote_addr = remote_addrs[remote];
      // Execute write over the connection to the remote node
      remote_addr.addr_info.offset = 0;  // Hardcoding zero offset for contended
      remote_addr.addr_info.length = kChunkSize;
      for (int wr_id = 0; wr_id < kNumIters; wr_id++) {
        r_collector_v2->ops++;
        r_collector_v2->bytes += kChunkSize * 2;
        auto now = std::chrono::steady_clock::now();
        remote_conn->Read(read_to_addr, remote_addr, wr_id);
        ROMULUS_ASSERT(remote_conn->ProcessCompletions(1) == 1,
                       "Failed to process completion");
        r_collector_v2->times.push_back(
            std::chrono::duration<double, std::micro>(
                std::chrono::steady_clock::now() - now)
                .count());
      }
      r_collector_v2->total_time_s =
          std::chrono::duration<double>(std::chrono::steady_clock::now() -
                                        read_start_v2)
              .count();
    }
    auto r_stats = stats::digest(r_collector_v2.get());
    r_stats.log_csv(outfile);
    mgr.arrive_strict_barrier();

    // ------------------- Contended CAS -------------------
    cas_to_addr.length = sizeof(uint64_t);
    std::unique_ptr<stats::collector_t> c_collector_v2 =
        std::make_unique<stats::collector_t>();
    c_collector_v2->op_type = "Contended CAS";
    auto cas_start_v2 = std::chrono::steady_clock::now();
    for (auto& remote : remotes) {
      auto remote_conn = remote_conns[remote];
      auto remote_addr = remote_addrs[remote];
      // Execute write over the connection to the remote node
      remote_addr.addr_info.offset = 0;  // Hardcoding zero offset for contended
      remote_addr.addr_info.length = sizeof(uint64_t);
      for (int wr_id = 0; wr_id < kNumIters; wr_id++) {
        c_collector_v2->ops++;
        c_collector_v2->bytes += sizeof(uint64_t);
        auto now = std::chrono::steady_clock::now();
        remote_conn->CompareAndSwap(cas_to_addr, remote_addr, 0, 1, wr_id);
        ROMULUS_ASSERT(remote_conn->ProcessCompletions(1) == 1,
                       "Failed to process completion");
        c_collector_v2->times.push_back(
            std::chrono::duration<double, std::micro>(
                std::chrono::steady_clock::now() - now)
                .count());
      }
      c_collector_v2->total_time_s =
          std::chrono::duration<double>(std::chrono::steady_clock::now() -
                                        cas_start_v2)
              .count();
    }
    auto c_stats_v2 = stats::digest(c_collector_v2.get());
    c_stats_v2.log_csv(outfile);
    mgr.arrive_strict_barrier();

    ROMULUS_INFO(
        "\nUncontended Write stats: {}\nUncontended Read stats: "
        "{}\nUncontended "
        "CAS stats: {}\nContended Write stats: {}\nContended Read stats: "
        "{}\nContended CAS stats: {}",
        w_stats.ToString(), r_stats.ToString(), c_stats.ToString(),
        w_stats_v2.ToString(), r_stats_v2.ToString(), c_stats_v2.ToString());
    delete[] raw;
    delete[] local_raw;
  };
  // Launch the workers
  for (int exp = 3; exp <= EXP_MAX; exp++) {
    uint64_t payload_sz = 1ULL << exp;
    work(payload_sz);
  }
  ROMULUS_INFO("Done. Cleaning up...");
}
