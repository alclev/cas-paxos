#include "velos_squared.h"

std::vector<double> latencies;
std::vector<txn_t<int>> proposals;
std::unique_ptr<VelosSquared> vsq;
uint64_t i = 0;

#define INIT_CONSENSUS(transport_flag, mach_map)                               \
  ROMULUS_INFO("Using Velos^2...");                                            \
  auto registry = std::make_unique<romulus::ConnectionRegistry>(               \
    "VelosSquared", registry_ip);                                              \
  auto device = std::make_shared<romulus::Device>(transport_flag);             \
  vsq = std::make_unique<VelosSquared>(args, system_size, device);             \
  vsq->Init(dev_name, dev_port, std::move(registry), mach_map);

#define EXEC_LATENCY                                                           \
  [&]() {                                                                      \
    ROMULUS_DEBUG("Executing proposal {}...", i);                              \
    txn_t<int> txn = proposals[i++ % proposals.size()];                        \
    ROMULUS_ASSERT(!txn.keys.empty(),                                          \
                   "Transaction must have at least one key.");                 \
    uint64_t target_shard = vsq->SelectShard(txn.keys.front());                \
    ROMULUS_DEBUG("Txn is mapped to shard {}", target_shard);                  \
    Value v(txn.values.front());                                               \
    auto start = std::chrono::high_resolution_clock::now();                    \
    vsq->Propose(target_shard, v);                                             \
    auto end = std::chrono::high_resolution_clock::now();                      \
    latencies.push_back(                                                       \
      std::chrono::duration_cast<std::chrono::microseconds>(end - start)       \
        .count());                                                             \
  };

#define SYNC_NODES [&]() { vsq->Sync(); };

#define DONE [&]() { vsq->Shutdown(); };

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