#pragma once

#include <romulus/romulus.h>

#include <fstream>

#include "crash-consensus.h"
#include "external.h"

#define INIT_CONSENSUS(transport_flag, buf_sz, mach_map)                       \
  ROMULUS_INFO("Initializing Mu");                                             \
  std::vector<int> remote_ids;                                                 \
  for (int i = 1; i < (int)system_size + 1; ++i) {                             \
    if (i != id + 1) {                                                         \
      remote_ids.push_back(i);                                                 \
      ROMULUS_INFO("remote: {}", i);                                           \
    }                                                                          \
  }                                                                            \
  dory::Consensus mu(id + 1, remote_ids, outstanding_reqs);                    \
  mu.commitHandler([]([[maybe_unused]] bool leader,                            \
                      [[maybe_unused]] uint8_t *buf,                           \
                      [[maybe_unused]] size_t len) {});                        \
  auto registry =                                                              \
    std::make_unique<romulus::ConnectionRegistry>("MuTest", registry_ip);      \
  auto conn_manager = std::make_unique<romulus::ConnectionManager>(            \
    hostname, registry.get(), id, system_size, num_qps, num_shared_cq);

std::vector<double> latencies;

#define SYNC_NODES [&]() { conn_manager->arrive_strict_barrier(); };

#define EXEC_LATENCY                                                           \
  [&]() {                                                                      \
    uint32_t i = latencies.size() % kNumProposals;                             \
    auto start = std::chrono::steady_clock::now();                             \
    dory::ProposeError err;                                                    \
    if ((err = mu.propose(proposals[i].second, proposals[i].first)) !=         \
        dory::ProposeError::NoError) {                                         \
      i -= 1;                                                                  \
      switch (err) {                                                           \
      case dory::ProposeError::FastPath:                                       \
      case dory::ProposeError::FastPathRecyclingTriggered:                     \
      case dory::ProposeError::SlowPathCatchFUO:                               \
      case dory::ProposeError::SlowPathUpdateFollowers:                        \
      case dory::ProposeError::SlowPathCatchProposal:                          \
      case dory::ProposeError::SlowPathUpdateProposal:                         \
      case dory::ProposeError::SlowPathReadRemoteLogs:                         \
      case dory::ProposeError::SlowPathWriteAdoptedValue:                      \
      case dory::ProposeError::SlowPathWriteNewValue:                          \
        ROMULUS_FATAL("Error: in leader mode. Code: {}",                       \
                      static_cast<int>(err));                                  \
        break;                                                                 \
      case dory::ProposeError::SlowPathLogRecycled:                            \
        ROMULUS_FATAL("Log recycled, waiting a bit...");                       \
        std::this_thread::sleep_for(std::chrono::seconds(1));                  \
        break;                                                                 \
      case dory::ProposeError::MutexUnavailable:                               \
      case dory::ProposeError::FollowerMode:                                   \
        ROMULUS_FATAL("Error: in follower mode. Potential "                    \
                      "leader: {}",                                            \
                      mu.potentialLeader());                                   \
        break;                                                                 \
      default:                                                                 \
        ROMULUS_FATAL("Bug in code. You should only handle "                   \
                      "errors here");                                          \
      }                                                                        \
    } else {                                                                   \
      auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(     \
                       std::chrono::steady_clock::now() - start)               \
                       .count();                                               \
      double elapsed_ns = static_cast<double>(elapsed);                        \
      latencies.emplace_back(elapsed_ns);                                      \
    }                                                                          \
  };

#define DONE_LATENCY [&]() { outfile.close(); };

#define CALC_LAT                                                               \
  [&](std::tuple<double, double, double, double> *result,                      \
      std::vector<double> &latencies) {                                        \
    /* remove the first 25% of latencies as warmup */                          \
    latencies.erase(latencies.begin(),                                         \
                    latencies.begin() + latencies.size() / 4);                 \
    double latency_avg = 0.0;                                                  \
    double latency_stddev = 0.0;                                               \
    double latency_50p = 0.0;                                                  \
    double latency_99p = 0.0;                                                  \
    double latency_99_9p = 0.0;                                                \
    [[maybe_unused]] double latency_max = 0.0;                                 \
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
        std::distance(latencies.begin(),                                       \
                      std::max_element(latencies.begin(), latencies.end()));   \
      latency_max = latencies[latency_max_idx];                                \
      std::sort(latencies.begin(), latencies.end());                           \
      latency_50p =                                                            \
        latencies[static_cast<uint32_t>((latencies.size() * .50))];            \
      latency_99p =                                                            \
        latencies[static_cast<uint32_t>((latencies.size() * .99))];            \
      latency_99_9p =                                                          \
        latencies[static_cast<uint32_t>((latencies.size() * .999))];           \
      *result =                                                                \
        std::make_tuple(latency_avg, latency_50p, latency_99p, latency_99_9p); \
    }                                                                          \
  }

#define RESET [&]() {};

bool stopwatch_running = false;
std::vector<double> runtimes;
std::vector<uint64_t> counts;
uint64_t count = 0;

#define INIT_THROUGHPUT [&]() {};

#define EXEC_THROUGHPUT                                                        \
  [&]() {                                                                      \
    if (!stopwatch_running) {                                                  \
      count = 0;                                                               \
      ROMULUS_STOPWATCH_START();                                               \
      stopwatch_running = true;                                                \
    }                                                                          \
    uint32_t i = count % proposals.size();                                     \
    mu.propose(proposals[i].second, proposals[i].first);                       \
    ++count;                                                                   \
  };

#define DONE_THROUGHPUT                                                        \
  [&]() {                                                                      \
    if (stopwatch_running) {                                                   \
      runtimes.push_back(ROMULUS_STOPWATCH_SPLIT(ROMULUS_MICROSECONDS));       \
      counts.push_back(count);                                                 \
      stopwatch_running = false;                                               \
    }                                                                          \
  };

#define CALC_THROUGHPUT                                                        \
  [&](std::ofstream &outfile) {                                                \
    double avg_throughput = 0.0;                                               \
    uint32_t total_count = 0;                                                  \
    assert(runtimes.size() == counts.size() && runtimes.size());               \
    ROMULUS_INFO("Dumping counts and runtimes:");                              \
    for (uint32_t i = 0; i < runtimes.size(); ++i) {                           \
      ROMULUS_INFO("!> [THRU] count={} runtime={}", counts[i], runtimes[i]);   \
      avg_throughput += (counts[i] / runtimes[i]);                             \
    }                                                                          \
    total_count = std::accumulate(counts.begin(), counts.end(), 0);            \
    avg_throughput /= runtimes.size();                                         \
    outfile << avg_throughput << std::endl;                                    \
    ROMULUS_INFO("!> [THRU] throughput={:4.2f}ops/us", avg_throughput);        \
    ROMULUS_INFO("!> [THRU] count={}", total_count);                           \
  };