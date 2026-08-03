#include "mu_squared.h"

std::vector<double> latencies;
std::vector<txn_t<int>> proposals;
std::unique_ptr<MuSquared> msq;

#define INIT_CONSENSUS(transport_flag, buf_sz, mach_map)                       \
  ROMULUS_INFO("Initializing Mu Squared...");                                  \
  auto registry =                                                              \
      std::make_unique<romulus::ConnectionRegistry>("MuSquared", registry_ip); \
  auto device = std::make_shared<romulus::Device>(transport_flag);             \
  msq = std::make_unique<MuSquared>(args, system_size, device);                \
  msq->Init(dev_name, dev_port, std::move(registry), mach_map);                \
  proposals = msq->GetProposals();

#define LEASE_EXEC_LATENCY                                                 \
  [&]() {                                                                  \
    uint32_t i = latencies.size() % proposals.size();                      \
    ROMULUS_DEBUG("Executing proposal {}...", i);                           \
    auto start = std::chrono::high_resolution_clock::now();                \
    msq->Propose(proposals[i]);                                            \
    auto end = std::chrono::high_resolution_clock::now();                  \
    latencies.push_back(                                                   \
        std::chrono::duration_cast<std::chrono::microseconds>(end - start) \
            .count());                                                     \
  };

#define LEASE_SYNC_NODES [&]() { msq->Sync(); };

#define LEASE_DONE [&]() { msq->Cleanup(); };

#define CALC_LAT                                                               \
  [&](std::tuple<double, double, double, double>* result,                      \
      std::vector<double>& latencies) {                                        \
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
      *result = std::make_tuple(latency_avg, latency_50p, latency_99p,         \
                                latency_99_9p);                                \
    }                                                                          \
  }