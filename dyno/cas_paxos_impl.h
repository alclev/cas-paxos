#include <memory>

#include "cas_paxos_st.h"

std::unique_ptr<Paxos> paxos;

#define INIT_CONSENSUS()                                                    \
  DYNO_INFO("Initializing CAS-Paxos");                                      \
  auto registry =                                                           \
      std::make_unique<dyno::ConnectionRegistry>("PaxosTest", registry_ip); \
  if (reset_barrier >= 0) {                                                 \
    registry->SetBarrier(reset_barrier);                                    \
    DYNO_INFO("Barrier reset. Start launching program on other nodes.");    \
  }                                                                         \
  paxos = std::make_unique<PAXOS_NS::CasPaxos>(capacity, hostname.data(),   \
                                               host_id, remotes);           \
  reinterpret_cast<PAXOS_NS::CasPaxos*>(paxos.get())                        \
      ->Init(dev_name, dev_port + 1, std::move(registry), leader_fixed);

std::vector<double> latencies;

#define INIT_LATENCY [&]() { paxos->SyncNodes(); };

#define EXEC_LATENCY                                                 \
  [&]() {                                                            \
    uint32_t i = latencies.size() % kNumProposals;                   \
    ROME_STOPWATCH_START();                                          \
    paxos->Propose(proposals[i].first, proposals[i].second);         \
    latencies.emplace_back(ROME_STOPWATCH_SPLIT(ROME_MICROSECONDS)); \
  };

#define DONE_LATENCY []() { paxos->CatchUp(); };

#define CALC_LATENCY                                                           \
  [&]() {                                                                      \
    double latency_avg = 0.0;                                                  \
    double latency_stddev = 0.0;                                               \
    double latency_50p = 0.0;                                                  \
    double latency_99p = 0.0;                                                  \
    double latency_99_9p = 0.0;                                                \
    double latency_max = 0.0;                                                  \
    int latency_max_idx = 0;                                                   \
    if (latencies.size() > 0) {                                                \
      for (auto& l : latencies) {                                              \
        DYNO_INFO("!> [LAT] l={:4.2f} us", l);                                 \
      }                                                                        \
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
    DYNO_INFO("!> [LAT] count={}", latencies.size());                          \
    DYNO_INFO("!> [LAT] lat_avg={:4.2f} ± {:4.2f} us", latency_avg,            \
              latency_stddev);                                                 \
    DYNO_INFO("!> [LAT] lat_50p={:4.2f} us", latency_50p);                     \
    DYNO_INFO("!> [LAT] lat_99p={:4.2f} us", latency_99p);                     \
    DYNO_INFO("!> [LAT] lat_99_9p={:4.2f} us", latency_99_9p);                 \
    DYNO_INFO("!> [LAT] lat_max={:4.2f} us", latency_max);                     \
    DYNO_INFO("!> [LAT] lat_max_idx={}", latency_max_idx);                     \
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
      ROME_STOPWATCH_START();                                \
      stopwatch_running = true;                              \
    }                                                        \
    uint32_t i = count % proposals.size();                   \
    paxos->Propose(proposals[i].first, proposals[i].second); \
    ++count;                                                 \
  };

#define DONE_THROUGHPUT                                            \
  [&]() {                                                          \
    if (stopwatch_running) {                                       \
      runtimes.push_back(ROME_STOPWATCH_SPLIT(ROME_MICROSECONDS)); \
      counts.push_back(count);                                     \
      stopwatch_running = false;                                   \
    }                                                              \
    paxos->CatchUp();                                              \
  };

#define CALC_THROUGHPUT                                              \
  [&]() {                                                            \
    double avg_throughput = 0.0;                                     \
    uint32_t total_count = 0;                                        \
    assert(runtimes.size() == counts.size());                        \
    for (uint32_t i = 0; i < runtimes.size(); ++i) {                 \
      avg_throughput += (counts[i] / runtimes[i]);                   \
    }                                                                \
    total_count = std::accumulate(counts.begin(), counts.end(), 0);  \
    avg_throughput /= runtimes.size();                               \
    DYNO_INFO("!> [THRU] throughput={:4.2f}ops/us", avg_throughput); \
    DYNO_INFO("!> [THRU] count={}", total_count);                    \
    paxos->CleanUp();                                                \
  };
