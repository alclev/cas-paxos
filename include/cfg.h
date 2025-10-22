#pragma once

#include "cli.h"

namespace romulus {
constexpr const char* NODE_ID = "--node-id";
constexpr const char* HOSTNAME = "--hostname";
constexpr const char* REMOTES = "--remotes";
constexpr const char* REGISTRY_IP = "--registry-ip";
constexpr const char* TRANSPORT_TYPE = "--transport-type";
constexpr const char* DEV_NAME = "--dev-name";
constexpr const char* DEV_PORT = "--dev-port";
constexpr const char* TESTTIME = "--testtime";
constexpr const char* LOOP = "--loop";
constexpr const char* CAPACITY = "--capacity";
constexpr const char* BUF_SIZE = "--buf-size";
constexpr const char* SLEEP = "--sleep";
constexpr const char* LEADER_FIXED = "--leader-fixed";
constexpr const char* POLICY = "--policy";
constexpr const char* DURATION = "--duration";
constexpr const char* OUTPUT_FILE = "--output-file";
constexpr const char* HELP = "--help";

inline auto ARGS = {
    U64_ARG_OPT(NODE_ID, "Numerical identifier for this node.", 0),
    STR_ARG_OPT(HOSTNAME, "Hostname of this node.", ""),
    STR_ARG_OPT(REMOTES, "Comma-separated list of other nodes.", ""),
    STR_ARG_OPT(REGISTRY_IP, "IP address of the connection registry.",
                "10.10.1.1"),
    ENUM_ARG_OPT(TRANSPORT_TYPE, "RDMA transport type.", "IB", {"IB", "ROCE"}),
    STR_ARG_OPT(DEV_NAME, "Name of the RDMA device to use.", ""),
    U64_ARG_OPT(DEV_PORT, "Port number of the RDMA device.", 33330),
    U64_ARG_OPT(TESTTIME, "Experiment duration in seconds", 10),
    U64_ARG_OPT(LOOP, "Number of iterations between runtime checks.", 1000),
    U64_ARG_OPT(CAPACITY, "Capacity of the replicated log.", 10000000),
    U64_ARG_OPT(BUF_SIZE, "Buffer size for remote writes.", 64),
    U64_ARG_OPT(SLEEP, "Sleep interval between proposals in ms", 100),
    BOOL_ARG_OPT(LEADER_FIXED,
                 "If true, only a single node proposes commands."),
    ENUM_ARG_OPT(POLICY, "Leader rotation policy when not fixed.", "rotating",
                 {"rotating", "all"}),
    U64_ARG_OPT(DURATION, "Duration of leadership in rotating policy in ms.", 100),
    STR_ARG_OPT(OUTPUT_FILE, "File to output results to.", "stats.csv"),
    BOOL_ARG_OPT(HELP, "Print usage information.")};
}  // namespace romulus
