#pragma once

#include <romulus/cli.h>

using namespace romulus;

constexpr const char* HOSTNAME = "--hostname";
constexpr const char* TESTTIME = "--testtime";
constexpr const char* LOOP = "--loop";
constexpr const char* CAPACITY = "--capacity";
constexpr const char* SLEEP = "--sleep";
constexpr const char* DURATION = "--duration";
constexpr const char* KEY_RANGE = "--key-range";
constexpr const char* NUM_SHARDS = "--num-shards";
constexpr const char* PIPELINE_DEPTH = "--pipeline-depth";
constexpr const char* TXN_SIZE = "--txn-size";
constexpr const char* NUM_HANDLERS = "--num-handlers";
constexpr const char* NO_OUTLIERS = "--no-outliers";



// Cloudlab notes:
// r320
// - device: mlx4_0
// - port 1: IB
// - port 2: RoCE

// xl170
// - device: mlx5_0 (10 Gbps)
//  - port 1: RoCE
// - device: mlx5_3 (25 Gbps)
//  - port 1: RoCE

inline auto EXTRA_ARGS = {
    STR_ARG(HOSTNAME, "Hostname of this node."),
    U64_ARG_OPT(TESTTIME, "Experiment duration in seconds", 4),
    U64_ARG_OPT(LOOP, "Number of iterations between runtime checks.", 1000),
    U64_ARG_OPT(CAPACITY, "Capacity of the replicated log.", 1048583),
    U64_ARG_OPT(SLEEP, "Sleep interval between proposals in ms", 0),
    U64_ARG_OPT(KEY_RANGE, "Key range for the workload.", 256),
    U64_ARG_OPT(NUM_SHARDS, "Number of shards for the workload.", 20),
    U64_ARG_OPT(PIPELINE_DEPTH, "Consensus pipeline depth.", 1),
    U64_ARG_OPT(TXN_SIZE, "Transaction size in terms of number of keys", 1),
    U64_ARG_OPT(NUM_HANDLERS, "Number of handler threads for memory permissioning", 4),
    BOOL_ARG_OPT(NO_OUTLIERS, "Use a workload without outliers (ideally routed)")
};
