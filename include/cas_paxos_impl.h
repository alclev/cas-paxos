#include <cassert>
#include <chrono>
#include <fstream>
#include <memory>

#include "cas_paxos_st.h"

std::unique_ptr<Paxos> paxos;

#define INIT_CONSENSUS(transport_flag, buf_sz, mach_map)                       \
  ROMULUS_INFO("Initializing CAS-Paxos");                                      \
  auto registry =                                                              \
      std::make_unique<romulus::ConnectionRegistry>("PaxosTest", registry_ip); \
  paxos = std::make_unique<PAXOS_NS::CasPaxos>(args, remotes, transport_flag); \
  reinterpret_cast<PAXOS_NS::CasPaxos*>(paxos.get())                           \
      ->Init(dev_name, dev_port, std::move(registry), mach_map);

std::vector<double> latencies;

#define SYNC_NODES [&]() { paxos->SyncNodes(); };

#define EXEC_LATENCY                                                      \
  [&]() {                                                                 \
    uint32_t i = latencies.size() % kNumProposals;                        \
    auto start = std::chrono::steady_clock::now();                        \
    paxos->Propose(proposals[i].first, proposals[i].second);              \
    auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>( \
                       std::chrono::steady_clock::now() - start)          \
                       .count();                                          \
    double elapsed_us = static_cast<double>(elapsed);                     \
    latencies.emplace_back(elapsed_us);                                   \
  };

#define DONE_LATENCY []() { paxos->CatchUp(); };

#define CALC_LATENCY                                                           \
  [&](std::fstream& outfile) {                                                 \
    double latency_avg = 0.0;                                                  \
    double latency_stddev = 0.0;                                               \
    double latency_50p = 0.0;                                                  \
    double latency_99p = 0.0;                                                  \
    double latency_99_9p = 0.0;                                                \
    double latency_max = 0.0;                                                  \
    int latency_max_idx = 0;                                                   \
    if (latencies.size() > 0) {                                                \
      latency_avg = std::accumulate(latencies.begin(), latencies.end(), 0.0);  \
      latency_avg /= static_cast<double>(latencies.size());                    \
      latency_stddev = std::accumulate(latencies.begin(), latencies.end(), 0,  \
                                       [latency_avg](double a, double b) {     \
                                         return a + std::abs(latency_avg - b); \
                                       });                                     \
      latency_stddev /= static_cast<double>(latencies.size());                 \
      latency_stddev = std::sqrt(latency_stddev);                              \
      latency_max_idx =                                                        \
          std::distance(latencies.begin(),                                     \
                        std::max_element(latencies.begin(), latencies.end())); \
      latency_max = latencies[latency_max_idx];                                \
      std::sort(latencies.begin(), latencies.end());                           \
      latency_50p =                                                            \
          latencies[static_cast<uint32_t>((latencies.size() * .50))];          \
      latency_99p =                                                            \
          latencies[static_cast<uint32_t>((latencies.size() * .99))];          \
      latency_99_9p =                                                          \
          latencies[static_cast<uint32_t>((latencies.size() * .999))];         \
    }                                                                          \
    outfile << latency_avg << "," << latency_50p << "," << latency_99p << ","  \
            << latency_99_9p << ",";                                           \
    ROMULUS_INFO("!> [LAT] count={}", latencies.size());                       \
    ROMULUS_INFO("!> [LAT] lat_avg={:4.2f} ± {:4.2f} us", latency_avg,         \
                 latency_stddev);                                              \
    ROMULUS_INFO("!> [LAT] lat_50p={:4.2f} us", latency_50p);                  \
    ROMULUS_INFO("!> [LAT] lat_99p={:4.2f} us", latency_99p);                  \
    ROMULUS_INFO("!> [LAT] lat_99_9p={:4.2f} us", latency_99_9p);              \
    ROMULUS_INFO("!> [LAT] lat_max={:4.2f} us", latency_max);                  \
    ROMULUS_INFO("!> [LAT] lat_max_idx={}", latency_max_idx);                  \
  };

#define RESET           \
  [&]() {               \
    paxos->Reset();     \
    paxos->SyncNodes(); \
  };

bool stopwatch_running = false;
std::vector<double> runtimes;
std::vector<uint64_t> counts;
uint64_t count = 0;

#define INIT_THROUGHPUT [&]() { paxos->SyncNodes(); };

#define EXEC_THROUGHPUT                                      \
  [&]() {                                                    \
    if (!stopwatch_running) {                                \
      count = 0;                                             \
      ROMULUS_STOPWATCH_START();                             \
      stopwatch_running = true;                              \
    }                                                        \
    uint32_t i = count % proposals.size();                   \
    paxos->Propose(proposals[i].first, proposals[i].second); \
    ++count;                                                 \
  };

#define DONE_THROUGHPUT                                                  \
  [&]() {                                                                \
    if (stopwatch_running) {                                             \
      runtimes.push_back(ROMULUS_STOPWATCH_SPLIT(ROMULUS_MICROSECONDS)); \
      counts.push_back(count);                                           \
      stopwatch_running = false;                                         \
    }                                                                    \
    paxos->CatchUp();                                                    \
  };

#define CALC_THROUGHPUT                                                      \
  [&](std::fstream& outfile) {                                               \
    double avg_throughput = 0.0;                                             \
    uint32_t total_count = 0;                                                \
    assert(runtimes.size() == counts.size());                                \
    ROMULUS_INFO("Dumping counts and runtimes:");                            \
    for (uint32_t i = 0; i < runtimes.size(); ++i) {                         \
      ROMULUS_INFO("!> [THRU] count={} runtime={}", counts[i], runtimes[i]); \
      avg_throughput += (counts[i] / runtimes[i]);                           \
    }                                                                        \
    total_count = std::accumulate(counts.begin(), counts.end(), 0);          \
    avg_throughput /= runtimes.size();                                       \
    outfile << avg_throughput << std::endl;                                  \
    ROMULUS_INFO("!> [THRU] throughput={:4.2f}ops/us", avg_throughput);      \
    ROMULUS_INFO("!> [THRU] count={}", total_count);                         \
  };
