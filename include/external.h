#pragma once
#include <atomic>
#include <chrono>
extern std::chrono::steady_clock::time_point failover_start_time;
extern std::atomic<bool> is_leader;