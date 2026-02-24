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

#if defined(USE_MU)
#include "mu/mu_impl.h"
#elif defined(USE_VELOS)
#include "velos/velos_impl.h"
#else
#include "cas_paxos_impl.h"
#endif

#define PAXOS_NS paxos_st
constexpr uint32_t kNumProposals = 8092;

void signal_handler(int signum) {
  if (signum == SIGTSTP) {
    write(STDOUT_FILENO, "SIGINT caught\n", 14);
    dump_requested.store(true, std::memory_order_relaxed);
  }
}

int main(int argc, char* argv[]) {
  struct sigaction sa{};
  sa.sa_handler = signal_handler;
  sigemptyset(&sa.sa_mask);
  sa.sa_flags = 0;
  sigaction(SIGTSTP, &sa, nullptr);

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

  INIT_CONSENSUS(transport_flag, buf_size, mach_map);
  FILL_PROPOSALS();

  std::function<void(void)> init = SYNC_NODES;
  std::function<void(void)> exec = EXEC_LATENCY;
  std::function<void(void)> done = DONE_LATENCY;
  std::function<void(std::ofstream&)> calc = CALC_LATENCY;
  std::function<void(void)> reset = RESET;

  init();

  ROMULUS_INFO("Starting latency test");
  ROMULUS_INFO("MultiPaxos Optimization: {}", multipax_opt ? "ON" : "OFF");

  std::atomic<bool> preprepare_running(true);

#if defined(USE_VELOS)
  ROMULUS_INFO("Using Velos, launching background thread...");

  std::thread([&]() {
    while (preprepare_running.load()) {
      // Running on fixed leader (Node0)
      if (id == 0) {
        velos->Prepare();
      }
    }
  }).detach();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));
#endif

  auto testtime_us =
      std::chrono::duration_cast<std::chrono::microseconds>(testtime);
  ROMULUS_STOPWATCH_BEGIN();
  while (ROMULUS_STOPWATCH_RUNTIME(ROMULUS_MICROSECONDS) <
         static_cast<uint64_t>(testtime_us.count())) {
    for (uint32_t i = 0; i < loop; ++i) {
#if defined(USE_MU) || defined(USE_VELOS)
      if (id == 0) {
        exec();
        busy_wait(sleep);
      }
#else
      if (paxos->isLeaderStable() && !paxos->isLeader()) {
        paxos->CatchUp();
      } else {
        exec();
      }
      busy_wait(sleep);
#endif
      if (dump_requested.load(std::memory_order_relaxed)) {
        ROMULUS_INFO("Shutdown requested, dumping logs...");
        velos->DumpLogs();
        exit(0);
      }
    }
  }
  preprepare_running.store(false);

  done();  // cleanup

  init();  // sync

  if (latencies.size() > 50) {
    calc(outfile);
    calc = CALC_THROUGHPUT;
    calc(outfile);
  }

  DUMP_LATENCIES();

  ROMULUS_INFO("Experiment is finished. Cleaning up...");
  outfile.close();
  for (auto& p : proposals) {
    delete[] p.second;
  }
  return 0;
}