#pragma once

#include <fstream>

#include "crash-consensus.h"

#define INIT_CONSENSUS(transport_flag, buf_sz)       \
  ROMULUS_INFO("Initializing Mu");                   \
  std::vector<int> remote_ids;                       \
  for (int i = 1; i < system_size + 1; ++i) {        \
    if (i != host_id + 1) {                          \
      remote_ids.push_back(i);                       \
      ROMULUS_INFO("remote: {}", i);                 \
    }                                                \
  }                                                  \
  dory::Consensus mu(host_id + 1, remote_ids);       \
  mu.commitHandler([]([[maybe_unused]] bool leader,  \
                      [[maybe_unused]] uint8_t* buf, \
                      [[maybe_unused]] size_t len) {});

std::vector<double> latencies;

#define INIT_LATENCY [&]() {};

#define EXEC_LATENCY                                                        \
  [&]() {                                                                   \
    uint32_t i = latencies.size() % kNumProposals;                          \
    auto start = std::chrono::steady_clock::now();                          \
    dory::ProposeError err;                                                 \
    if ((err = mu.propose(proposals[i].second, proposals[i].first)) !=      \
        dory::ProposeError::NoError) {                                      \
      i -= 1;                                                               \
      switch (err) {                                                        \
        case dory::ProposeError::FastPath:                                  \
        case dory::ProposeError::FastPathRecyclingTriggered:                \
        case dory::ProposeError::SlowPathCatchFUO:                          \
        case dory::ProposeError::SlowPathUpdateFollowers:                   \
        case dory::ProposeError::SlowPathCatchProposal:                     \
        case dory::ProposeError::SlowPathUpdateProposal:                    \
        case dory::ProposeError::SlowPathReadRemoteLogs:                    \
        case dory::ProposeError::SlowPathWriteAdoptedValue:                 \
        case dory::ProposeError::SlowPathWriteNewValue:                     \
          ROMULUS_FATAL("Error: in leader mode. Code: {}",                  \
                        static_cast<int>(err));                             \
          break;                                                            \
        case dory::ProposeError::SlowPathLogRecycled:                       \
          ROMULUS_FATAL("Log recycled, waiting a bit...");                  \
          std::this_thread::sleep_for(std::chrono::seconds(1));             \
          break;                                                            \
        case dory::ProposeError::MutexUnavailable:                          \
        case dory::ProposeError::FollowerMode:                              \
          ROMULUS_FATAL(                                                    \
              "Error: in follower mode. Potential "                         \
              "leader: {}",                                                 \
              mu.potentialLeader());                                        \
          break;                                                            \
        default:                                                            \
          ROMULUS_FATAL(                                                    \
              "Bug in code. You should only handle "                        \
              "errors here");                                               \
      }                                                                     \
    } else {                                                                \
      auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>( \
                         std::chrono::steady_clock::now() - start)          \
                         .count();                                          \
      double elapsed_us = static_cast<double>(elapsed);                     \
      latencies.emplace_back(elapsed_us);                                   \
    }                                                                       \
  };

#define DONE_LATENCY []() {};

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

#define RESET [&]() {};

bool stopwatch_running = false;
std::vector<double> runtimes;
std::vector<uint64_t> counts;
uint64_t count = 0;

#define INIT_THROUGHPUT [&]() {};

#define EXEC_THROUGHPUT                                  \
  [&]() {                                                \
    if (!stopwatch_running) {                            \
      count = 0;                                         \
      ROMULUS_STOPWATCH_START();                         \
      stopwatch_running = true;                          \
    }                                                    \
    uint32_t i = count % proposals.size();               \
    mu.propose(proposals[i].second, proposals[i].first); \
    ++count;                                             \
  };

#define DONE_THROUGHPUT                                                  \
  [&]() {                                                                \
    if (stopwatch_running) {                                             \
      runtimes.push_back(ROMULUS_STOPWATCH_SPLIT(ROMULUS_MICROSECONDS)); \
      counts.push_back(count);                                           \
      stopwatch_running = false;                                         \
    }                                                                    \
  };

#define CALC_THROUGHPUT                                                      \
  [&](std::fstream& outfile) {                                               \
    double avg_throughput = 0.0;                                             \
    uint32_t total_count = 0;                                                \
    assert(runtimes.size() == counts.size() && runtimes.size());             \
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