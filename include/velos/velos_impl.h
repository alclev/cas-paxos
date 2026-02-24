#pragma once

#include "velos/velos_mt.h"

std::unique_ptr<Velos> velos;

#define INIT_CONSENSUS(transport_flag, buf_sz, mach_map)                       \
  ROMULUS_INFO("Initializing Velos");                                          \
  auto registry =                                                              \
      std::make_unique<romulus::ConnectionRegistry>("VelosTest", registry_ip); \
  velos = std::make_unique<Velos>(args, remotes, transport_flag);              \
  velos->Init(dev_name, dev_port, std::move(registry), mach_map);

std::vector<double> latencies;

#define EXEC_LATENCY                                                          \
  [&]() {                                                                     \
    uint32_t i = latencies.size() % kNumProposals;                            \
    auto start = std::chrono::steady_clock::now();                            \
    velos->Promise(Value(*reinterpret_cast<uint32_t*>(proposals[i].second))); \
    auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(     \
                       std::chrono::steady_clock::now() - start)              \
                       .count();                                              \
    double elapsed_us = static_cast<double>(elapsed);                         \
    latencies.emplace_back(elapsed_us);                                       \
  };

#define SYNC_NODES [&]() { velos->SyncNodes(); };

#define DONE_LATENCY []() { velos->CleanUp(); };

#define CALC_LATENCY                                                           \
  [&](std::ofstream& outfile) {                                                \
    double latency_avg = 0.0;                                                  \
    double latency_stddev = 0.0;                                               \
    double latency_50p = 0.0;                                                  \
    double latency_99p = 0.0;                                                  \
    double latency_99_9p = 0.0;                                                \
    double latency_max = 0.0;                                                  \
    int latency_max_idx = 0;                                                   \
    /* Get rid of the shutdown message latency */                              \
    for (int i = 0; i < 50; ++i) {                                             \
      latencies.pop_back();                                                    \
    }                                                                          \
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
    velos->Reset();     \
    velos->SyncNodes(); \
  };

#define CALC_THROUGHPUT                                             \
  [&](std::ofstream& outfile) {                                     \
    double total_latency =                                          \
        std::accumulate(latencies.begin(), latencies.end(), 0.0);   \
    double throughput = latencies.size() / total_latency * 1000000; \
    outfile << throughput << std::endl;                             \
    ROMULUS_INFO("!> [THRU] throughput={:4.2f}ops/us", throughput); \
  };

#define DUMP_LATENCIES()                            \
  {                                                 \
    std::ostringstream oss;                         \
    for (size_t i = 0; i < latencies.size(); ++i) { \
      if (i > 0) oss << ",";                        \
      oss << latencies[i];                          \
    }                                               \
    ROMULUS_INFO("Latencies: {}", oss.str());       \
  }
