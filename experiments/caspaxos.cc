#include <csignal>
#include <filesystem>
#include <functional>
#include <random>
#include <string>

#include "cfg.h"
#include "romulus/cfg.h"
#include "romulus/common.h"
#include "romulus/connection_manager.h"
#include "romulus/device.h"
#include "romulus/memblock.h"
#include "romulus/qp_pol.h"
#include "romulus/stats.h"
#include "romulus/util.h"
#include "state.h"
#include "util.h"
#include "workload.h"

#ifdef DEFAULT
#include "cas_paxos_impl.h"
#endif
#ifdef USE_MU
#include "mu/mu_impl.h"
#endif
#ifdef USE_VELOS
#include "velos/velos_impl.h"
#endif
#ifdef USE_LEASE
#include "cas_paxos_impl.h"
#include "lease_impl.h"
#include "mu_squared.h"
#endif

#if (defined(DEFAULT) && defined(USE_MU)) ||                                   \
  (defined(DEFAULT) && defined(USE_VELOS)) ||                                  \
  (defined(DEFAULT) && defined(USE_LEASE)) ||                                  \
  (defined(USE_MU) && defined(USE_VELOS)) ||                                   \
  (defined(USE_MU) && defined(USE_LEASE)) ||                                   \
  (defined(USE_VELOS) && defined(USE_LEASE))
#error "Conflicting options: only one mode can be selected at a time"
#endif

#define PAXOS_NS paxos_st
constexpr uint32_t kNumProposals = 8092;
constexpr double kThruFreq = 1e5; // us

int main(int argc, char *argv[]) {
  ROMULUS_STOPWATCH_DECLARE();

  romulus::INIT();
  auto args = std::make_shared<romulus::ArgMap>();
  args->import(romulus::ARGS);
  args->import(romulus::EXTRA_ARGS);
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
  ROMULUS_INFO("!> [CONF] buf_size={}", buf_size);
  ROMULUS_INFO("!> [CONF] sleep={}_ms", sleep.count());
  ROMULUS_INFO("!> [CONF] leader_fixed={}", leader_fixed);
  ROMULUS_INFO("!> [CONF] policy={}", policy);
  ROMULUS_INFO("!> [CONF] duration={}_ms", duration.count());
  ROMULUS_INFO("!> [CONF] system_size={}", system_size);
  ROMULUS_INFO("!> [CONF] output file={}", output_file);
  if (sleep.count() > 0)
    ROMULUS_INFO(
      "!> [WARNING] sleep={}_ms -- Do not run throughput tests with sleep "
      "enabled",
      sleep.count());

  INIT_CONSENSUS(transport_flag, buf_size, mach_map);
  WorkloadGenerator wg(args, key_range, kNumProposals, system_size);
  wg.generate();
  auto &proposals = wg.get_ops();
  // wg.print(0, 10);

  std::function<void(void)> sync = SYNC_NODES;
  std::function<void(void)> exec = EXEC_LATENCY;
  std::function<void(void)> done = DONE_LATENCY;
  std::function<void(std::tuple<double, double, double, double> *,
                     std::vector<double> &)>
    calc = CALC_LAT;
  std::function<void(void)> reset = RESET;

  ROMULUS_INFO("Starting latency test");

#ifdef DEFAULT
  ROMULUS_INFO("MultiPaxos Optimization: {}", multipax_opt ? "ON" : "OFF");
#ifdef FAILOVER
  auto fd_threads = paxos->FailureDetector();
  auto iterations = 0;
  auto testtime_us =
    std::chrono::duration_cast<std::chrono::microseconds>(testtime);
  uint64_t total_work_us = 0;
  paxos->Warmup();
  ROMULUS_STOPWATCH_BEGIN();
  while (ROMULUS_STOPWATCH_RUNTIME(ROMULUS_MICROSECONDS) <
         static_cast<uint64_t>(testtime_us.count())) {
    auto *failure_detected = paxos->isFailureDetected();

    for (uint32_t i = 0; i < loop; ++i) {
      ROMULUS_VERBOSE(
        "<Main> Loop i={}, log_offset={}, isLeader={}, isLeaderStable={}", i,
        paxos->GetOffset(), paxos->isLeader(), paxos->isLeaderStable());
      paxos->ConditionalReset();
      ROMULUS_VERBOSE("<Main> After ConditionalReset");
      if (paxos->isLeaderStable() && !paxos->isLeader()) {
        ROMULUS_VERBOSE("<Main> Follower path - calling CatchUp");
        paxos->CatchUp();
      } else if ((!paxos->isLeaderStable() && failure_detected->load()) ||
                 (!paxos->isLeaderStable() || paxos->isLeader())) {
        ROMULUS_VERBOSE("<Main> Leader path - calling exec");
        auto work_start = std::chrono::steady_clock::now();
        exec();
        auto work_end = std::chrono::steady_clock::now();
        iterations++;
        total_work_us += std::chrono::duration_cast<std::chrono::microseconds>(
                           work_end - work_start)
                           .count();
        ROMULUS_VERBOSE("<Main> exec returned");
      } else {
        ROMULUS_VERBOSE(
          "<Main> NEITHER BRANCH TAKEN! isLeader={}, isLeaderStable={}, "
          "isFailureDetected={}",
          paxos->isLeader(), paxos->isLeaderStable(), failure_detected->load());
      }

      ROMULUS_VERBOSE("<Main> About to busy_wait");

      busy_wait(sleep, failure_detected);
    }
    ROMULUS_VERBOSE("<Main> For loop completed, checking while condition");
  }
#endif
  // Regular path (no failover)
  // Warmup before starting the timer
  paxos->Warmup();

  auto election_start = std::chrono::steady_clock::now();
  // Assumption -- the first exec is the election round and will be successful
  if (paxos->MaybeLeaderId() == id)
    exec();
  [[maybe_unused]] auto election_lat =
    std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::steady_clock::now() - election_start);

  // Give preparer thread plenty of time to run ahead
  paxos->Preprepare();
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // exit(0);

  auto testtime_us =
    std::chrono::duration_cast<std::chrono::microseconds>(testtime);
  uint64_t total_work_us = 0;
  size_t iterations = 0;
  ROMULUS_STOPWATCH_BEGIN();
  while (ROMULUS_STOPWATCH_RUNTIME(ROMULUS_MICROSECONDS) <
         static_cast<uint64_t>(testtime_us.count())) {
    for (uint32_t i = 0; i < loop; ++i) {
      if (paxos->MaybeLeaderId() == id && paxos->GetOffset() < capacity) {
        exec();
        iterations++;
      }
    }
  }

  if (paxos->isLeader()) {
    std::tuple<double, double, double, double> result;
    auto election_lat = latencies.front();
    calc(&result, latencies);
    std::stringstream ss;
    ss << system_size << "," << total_work_us << "," << iterations << ","
       << election_lat << "," << std::get<0>(result) << ","
       << std::get<1>(result) << "," << std::get<2>(result) << ","
       << std::get<3>(result);
    ss << std::endl;
    ROMULUS_INFO(
      "Work time (us): {}\tTotal ops: {}\nLease election latency (ns): {}\nAvg "
      "latency (ns): {}\nP50 latency (ns): {}\nP99 latency (ns): {}\nP99.9 "
      "latency (ns): {}",
      total_work_us, iterations, election_lat, std::get<0>(result),
      std::get<1>(result), std::get<2>(result), std::get<3>(result));
    ROMULUS_INFO("[PARSE] {}", ss.str());
    // system_size, worktime_us, total_ops, election_lat, lat_avg, lat_50p,
    // lat_99p, lat_99_9p calc = CALC_THROUGHPUT; calc(outfile);
  }

  sync();

  ROMULUS_INFO("Experiment is finished. Cleaning up...");
  done(); // cleanup

#ifdef FAILOVER
  for (auto &t : fd_threads) {
    t.join();
  }
#endif

#endif

#ifdef USE_VELOS
  std::atomic<bool> preprepare_running(true);

  ROMULUS_INFO("Using Velos, launching background thread...");

  std::thread([&]() {
    while (preprepare_running.load()) {
      // Running on fixed leader (Node0)
      if (id == 0) {
        velos->Prepare();
      }
    }
  }).detach();

  double last_clock = 0;

  std::vector<double> thrus, lats;
  thrus.reserve(10);
  lats.reserve(10);
  uint64_t total_commits = 0;
  uint64_t commits = 0;

  // block until we prepare the entire log
  if (id == 0) {
    while (velos->PrepareOffset() < capacity) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  }

  sync();

  ROMULUS_INFO(
    "Preparer thread has prepared the entire log, starting experiment...");

  auto testtime_us =
    std::chrono::duration_cast<std::chrono::microseconds>(testtime);
  ROMULUS_STOPWATCH_BEGIN();
  
  auto start = std::chrono::steady_clock::now();
  while (std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::steady_clock::now() - start)
             .count() < testtime_us.count()) {
    if (id == 0) {
      auto curr_us = std::chrono::duration_cast<std::chrono::microseconds>(
                       std::chrono::steady_clock::now() - start)
                       .count();
      if (curr_us - last_clock > kThruFreq) {
        last_clock = curr_us;
        thrus.push_back(commits);
        lats.push_back(latencies.back());
        commits = 0; // reset
      }

      exec();

      commits++;
      total_commits++;
      busy_wait(sleep);
    }
  }
  double elapsed_us = std::chrono::duration<double, std::micro>(
                        std::chrono::steady_clock::now() - start).count();

  preprepare_running.store(false);
  sync();

  if (id == 0) {
    std::tuple<double, double, double, double> latency_stats;
    // dump latencies
    std::ofstream raw_lats_file("raw_lats_" + std::to_string(id) + ".txt");
    for (size_t i = 0; i < latencies.size(); ++i) {
      raw_lats_file << latencies[i];
      if (i != latencies.size() - 1) {
        raw_lats_file << ",";
      }
    }
    raw_lats_file << std::endl;

    // Collect the lat and thru stats
    std::stringstream thru_ss;
    for (size_t t = 0; t < thrus.size(); ++t) {
      thru_ss << thrus[t];
      if (t != thrus.size() - 1) {
        thru_ss << ",";
      }
    }
    thru_ss << std::endl;

    std::stringstream lat_ss;
    for (size_t t = 0; t < lats.size(); ++t) {
      lat_ss << lats[t];
      if (t != lats.size() - 1) {
        lat_ss << ",";
      }
    }
    lat_ss << std::endl;

    ROMULUS_INFO("[THROUGHPUTS] {}", thru_ss.str());
    ROMULUS_INFO("[LATENCIES] {}", lat_ss.str());

    CALC_LAT(&latency_stats, latencies);

    std::stringstream result_ss;
    result_ss << "\n\t\tNode id: " << id << "\n\t\tSystem size: " << system_size
              << "\n\t\tTotal commits: " << total_commits
              << "\n\t\tTotal worktime (us): " << static_cast<uint64_t>(elapsed_us)
              << "\n\t\tAvg. latency (us): " << std::get<0>(latency_stats)
              << ", "
              << "\n\t\tp50 latency (us): " << std::get<1>(latency_stats)
              << ", "
              << "\n\t\tp99 latency (us): " << std::get<2>(latency_stats)
              << ", "
              << "\n\t\tp99.9 latency (us): " << std::get<3>(latency_stats);
    result_ss << std::endl;
    ROMULUS_INFO("[Results] {}", result_ss.str());
    // id, system_size, total_commits, total_worktime_us, lat_avg, lat_50p,
    // lat_99p, lat_99_9p
    result_ss.str("");
    result_ss.clear();
    result_ss << id << "," << system_size << "," << total_commits << ","
              << static_cast<uint64_t>(elapsed_us) << "," << std::get<0>(latency_stats) << ","
              << std::get<1>(latency_stats) << "," << std::get<2>(latency_stats)
              << "," << std::get<3>(latency_stats);
    result_ss << std::endl;
    ROMULUS_INFO("[PARSE] {}", result_ss.str());
  }

  ROMULUS_INFO("Experiment is finished. Cleaning up...");
  done(); // cleanup
#endif

#ifdef USE_MU
  ROMULUS_INFO("Waiting for all nodes to be up...");
  std::this_thread::sleep_for(std::chrono::seconds(2 + system_size - id));

  auto testtime_us =
    std::chrono::duration_cast<std::chrono::microseconds>(testtime);
  ROMULUS_STOPWATCH_BEGIN();
  uint64_t total_worktime_us = 0;
  uint64_t total_commits = 0;
  uint64_t commits = 0;
  double last_clock = 0;
  std::vector<double> thrus, lats;
  thrus.reserve(10);
  lats.reserve(10);

  while (ROMULUS_STOPWATCH_RUNTIME(ROMULUS_MICROSECONDS) <
         static_cast<uint64_t>(testtime_us.count())) {
    // ROMULUS_INFO("Am I the leader? {}", is_leader.load() ? "Yes" : "No");
    // Lowest leader id will be elected first...
#ifndef FAILOVER
    if (id == 0) {
      double curr_us = ROMULUS_STOPWATCH_RUNTIME(ROMULUS_MICROSECONDS);

      if (curr_us - last_clock > kThruFreq) {
        last_clock = curr_us;
        thrus.push_back(commits);
        lats.push_back(latencies.back());
        commits = 0; // reset
      }

      // ROMULUS_INFO("[LEADER] Executing iteration {}", iterations);
      auto work_start = std::chrono::steady_clock::now();
      exec();
      total_worktime_us +=
        std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now() - work_start)
          .count();
      ++commits;
      ++total_commits;
    }
#else
    if (id == 0) {
      if (iterations > 100) {
        ROMULUS_INFO("Stalling leader... ");
        std::abort();
        goto stall_leader;
      }
      ROMULUS_INFO("[LEADER] Executing iteration {}", iterations);
      exec();
    }
    if (id != 0 && is_leader.load()) {
      auto failover_end_time = std::chrono::steady_clock::now();
      if (failover_start_time == std::chrono::steady_clock::time_point()) {
        ROMULUS_INFO("Failover start time was not set!");
      }
      auto failover_duration =
        std::chrono::duration_cast<std::chrono::microseconds>(
          failover_end_time - failover_start_time);
      ROMULUS_INFO("[FAILOVER]: {} us", failover_duration.count());
      goto stall_leader;
    }
#endif
    // busy_wait(sleep);
  }
#ifdef FAILOVER
stall_leader:
#endif
  sync();

  if (is_leader.load()) {
    std::tuple<double, double, double, double> result;
    // dump latencies
    std::ofstream raw_lats_file("raw_lats_" + std::to_string(id) + ".txt");
    for (size_t i = 0; i < latencies.size(); ++i) {
      raw_lats_file << latencies[i];
      if (i != latencies.size() - 1) {
        raw_lats_file << ",";
      }
    }
    raw_lats_file << std::endl;

    // Collect the lat and thru stats
    std::stringstream thru_ss;
    for (size_t t = 0; t < thrus.size(); ++t) {
      thru_ss << thrus[t];
      if (t != thrus.size() - 1) {
        thru_ss << ",";
      }
    }
    thru_ss << std::endl;

    std::stringstream lat_ss;
    for (size_t t = 0; t < lats.size(); ++t) {
      lat_ss << lats[t];
      if (t != lats.size() - 1) {
        lat_ss << ",";
      }
    }
    lat_ss << std::endl;

    ROMULUS_INFO("[THROUGHPUTS] {}", thru_ss.str());
    ROMULUS_INFO("[LATENCIES] {}", lat_ss.str());

    calc(&result, latencies);
    std::stringstream ss;
    ss << system_size << "," << outstanding_reqs << "," << total_worktime_us
       << "," << total_commits << "," << std::get<0>(result) << ","
       << std::get<1>(result) << "," << std::get<2>(result) << ","
       << std::get<3>(result);
    ss << std::endl;
    ROMULUS_INFO("[PARSE] {}", ss.str());
  }

  ROMULUS_INFO("Experiment is finished. Cleaning up...");
  done(); // cleanup
#endif

#ifdef USE_LEASE

  ROMULUS_INFO("Using Mu^2...");
  auto *cas = dynamic_cast<paxos_st::CasPaxos *>(paxos.release());
  auto mu_squared =
    std::make_unique<paxos_st::MuSquared>(std::move(*cas), proposals);

  exec = LEASE_EXEC_LATENCY;
  done = LEASE_DONE;
  sync = LEASE_SYNC_NODES;

  pin_thread_to_core(0);

  ROMULUS_INFO("Warming up...");
  mu_squared->Warmup();

  // First proposal is reserved for the lease
  auto lease_start = std::chrono::steady_clock::now();
  mu_squared->LeasePropose(proposals[0].first, proposals[0].second, true);
  auto lease_elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
                         std::chrono::steady_clock::now() - lease_start)
                         .count();
  // mu_squared->StartCommitThreads();
  ROMULUS_INFO("Lease election latency: {} ns", lease_elapsed);
  uint64_t total_work_us = 0;
  uint64_t rounds = 0;

  auto testtime_us =
    std::chrono::duration_cast<std::chrono::microseconds>(testtime);
  ROMULUS_STOPWATCH_BEGIN();
  while (ROMULUS_STOPWATCH_RUNTIME(ROMULUS_MICROSECONDS) <
         static_cast<uint64_t>(testtime_us.count())) {
    // first entry is the lease msg
    int i = 1 + (rounds % kNumProposals);
    auto work_start = std::chrono::high_resolution_clock::now();
    mu_squared->LeasePropose(proposals[i].first, proposals[i].second);
    auto work_end = std::chrono::high_resolution_clock::now();
    total_work_us += std::chrono::duration_cast<std::chrono::microseconds>(
                       work_end - work_start)
                       .count();
    busy_wait(sleep);
    rounds++;
  }
  sync();
  uint64_t total_ops = mu_squared->GetTotalOps();
  latencies = mu_squared->AggregateLatencies();
  std::tuple<double, double, double, double> latency_stats;
  // dump latencies
  std::stringstream csv_ss;

  // for (size_t i = 0; i < latencies.size(); ++i) {
  //   csv_ss << latencies[i];
  //   if (i != latencies.size() - 1) {
  //     csv_ss << ",";
  //   }
  // }
  // csv_ss << std::endl;
  // ROMULUS_INFO("[LATENCIES] {}", csv_ss.str());
  CALC_LAT(&latency_stats, latencies);
  // system_size, worktime_us, total_ops, election_lat, lat_avg, lat_50p,
  // lat_99p, lat_99_9p
  csv_ss.str("");
  csv_ss.clear();
  csv_ss << system_size << "," << total_work_us << "," << total_ops << ","
         << lease_elapsed << "," << std::get<0>(latency_stats) << ","
         << std::get<1>(latency_stats) << "," << std::get<2>(latency_stats)
         << "," << std::get<3>(latency_stats);
  csv_ss << std::endl;
  ROMULUS_INFO(
    "Work time (us): {}\tTotal ops: {}\nLease election latency (ns): {}\nAvg "
    "latency (ns): {}\nP50 latency (ns): {}\nP99 latency (ns): {}\nP99.9 "
    "latency (ns): {}",
    total_work_us, total_ops, lease_elapsed, std::get<0>(latency_stats),
    std::get<1>(latency_stats), std::get<2>(latency_stats),
    std::get<3>(latency_stats));
  ROMULUS_INFO("[PARSE] {}", csv_ss.str());

  ROMULUS_INFO("Experiment is finished. Cleaning up...");
  done(); // cleanup

#endif

  outfile.close();
  for (auto &p : proposals) {
    delete[] p.second;
  }

  return 0;
}