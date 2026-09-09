#include <csignal>
#include <filesystem>
#include <functional>
#include <random>
#include <string>

#include "cfg.h"
#include "lease_impl.h"
#include "mu_squared.h"
#include "romulus/cfg.h"
#include "romulus/common.h"
#include "romulus/connection_manager.h"
#include "romulus/device.h"
#include "romulus/memblock.h"
#include "romulus/qp_pol.h"
#include "romulus/romulus.h"
#include "romulus/stats.h"
#include "romulus/util.h"
#include "state.h"
#include "util.h"
#include "workload.h"

#ifdef MU_SQUARED
#include "msq_impl.h"
#else
#include "vsq_impl.h"
#endif

#define PAXOS_NS paxos_st

constexpr double kThruFreq = 1e6; // us

/// @brief
/// @param argc
/// @param argv
/// @return
int main(int argc, char *argv[]) {
  ROMULUS_STOPWATCH_DECLARE();

  romulus::INIT();
  auto args = std::make_shared<romulus::ArgMap>();
  args->import(romulus::ARGS);
  args->import(EXTRA_ARGS);
  args->parse(argc, argv);
  INGEST_ARGS(args);

  // Print configuration.
  ROMULUS_INFO("Experimental Configuration:");
  ROMULUS_INFO("!> [CONF] hostname={}", hostname);
  ROMULUS_INFO("!> [CONF] host id={}", id);
  ROMULUS_INFO("!> [CONF] registry ip={}", registry_ip);
  ROMULUS_INFO("!> [CONF] output file={}", output_file);
  ROMULUS_INFO("!> [CONF] testtime={}_s", testtime.count());
  ROMULUS_INFO("!> [CONF] device name={}", dev_name);
  ROMULUS_INFO("!> [CONF] device port={}", dev_port);
  ROMULUS_INFO("!> [CONF] transport type={}", transport);
  ROMULUS_INFO("!> [CONF] loop={}", loop);
  ROMULUS_INFO("!> [CONF] capacity={}", capacity);
  ROMULUS_INFO("!> [CONF] sleep={}_ms", sleep.count());
  ROMULUS_INFO("!> [CONF] system_size={}", system_size);
  if (sleep.count() > 0)
    ROMULUS_INFO(
        "!> [WARNING] sleep={}_ms -- Do not run throughput tests with sleep "
        "enabled",
        sleep.count());

  ROMULUS_INFO("Initializing Mu Squared...");
  INIT_CONSENSUS(transport_flag, mach_map);
  // msq->RemoteDump();

  std::function<void(void)> sync = SYNC_NODES;
  std::function<void(void)> exec = EXEC_LATENCY;
  std::function<void(void)> done = DONE;
  std::function<void(std::tuple<double, double, double, double> *,
                     std::vector<double> &)>
      calc = CALC_LAT;
  // std::function<void(void)> reset = RESET;

  pin_thread_to_core(0);

  // ROMULUS_INFO("Warming up...");
  // msq->Warmup();

  uint64_t commits = 0;
  uint64_t total_commits = 0;
  uint64_t total_worktime_us = 0;
  double last_thru_clock = 0;
  std::vector<double> thrus;
  thrus.reserve(10);

  auto testtime_us =
      std::chrono::duration_cast<std::chrono::microseconds>(testtime);
  ROMULUS_STOPWATCH_BEGIN();
  while (ROMULUS_STOPWATCH_RUNTIME(ROMULUS_MICROSECONDS) <
         static_cast<uint64_t>(testtime_us.count())) {
    double curr_us = ROMULUS_STOPWATCH_RUNTIME(ROMULUS_MICROSECONDS);
    if (curr_us - last_thru_clock > kThruFreq) {
      last_thru_clock = curr_us;
      thrus.push_back(commits);
      commits = 0; // reset
    }
    auto work_start = std::chrono::steady_clock::now();
    exec();
    total_worktime_us += std::chrono::duration_cast<std::chrono::microseconds>(
                             std::chrono::steady_clock::now() - work_start)
                             .count();
    commits++;
    total_commits++;
    busy_wait(sleep);
  }
  sync();

  std::tuple<double, double, double, double> latency_stats;
  std::stringstream result_ss;
  // dump latencies
  for (size_t i = 0; i < latencies.size(); ++i) {
    result_ss << latencies[i];
    if (i != latencies.size() - 1) {
      result_ss << ",";
    }
  }
  result_ss << std::endl;
  ROMULUS_INFO("[LATENCIES] {}", result_ss.str());
  result_ss.str("");
  result_ss.clear();
  // dump throughputs
  result_ss << id << ",";
  for (size_t t = 0; t < thrus.size(); ++t) {
    result_ss << thrus[t];
    if (t != thrus.size() - 1) {
      result_ss << ",";
    }
  }
  result_ss << std::endl;
  ROMULUS_INFO("[THROUGHPUTS] {}", result_ss.str());
  ROMULUS_DEBUG("[THROUGHPUTS] ((ops/sec) Freq: {} us) {}", kThruFreq,
                result_ss.str());
  result_ss.str("");
  result_ss.clear();

  CALC_LAT(&latency_stats, latencies);

  result_ss << "Node id" << id << " System size: " << system_size
            << " Total commits: " << total_commits
            << " Total worktime (us): " << total_worktime_us
            << " Avg. latency (us): " << std::get<0>(latency_stats) << ", "
            << "p50 latency (us): " << std::get<1>(latency_stats) << ", "
            << "p99 latency (us): " << std::get<2>(latency_stats) << ", "
            << "p99.9 latency (us): " << std::get<3>(latency_stats);
  result_ss << std::endl;
  ROMULUS_INFO("[Results] {}", result_ss.str());
  // id, system_size, total_commits, total_worktime_us, lat_avg, lat_50p,
  // lat_99p, lat_99_9p
  result_ss.str("");
  result_ss.clear();
  result_ss << id << "," << system_size << "," << total_commits << ","
            << total_worktime_us << "," << std::get<0>(latency_stats) << ","
            << std::get<1>(latency_stats) << "," << std::get<2>(latency_stats)
            << "," << std::get<3>(latency_stats);
  result_ss << std::endl;
  ROMULUS_INFO("[PARSE] {}", result_ss.str());

  ROMULUS_INFO("Experiment is finished. Cleaning up...");
  done(); // cleanup

  return 0;
}