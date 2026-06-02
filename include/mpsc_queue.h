#pragma once
#include <atomic>
#include <cstddef>

template <typename T, size_t N>
class MPSCQueue {
  static_assert((N & (N-1)) == 0, "N must be power of 2");

  struct Slot {
    alignas(64) std::atomic<size_t> seq;
    T val;
  };

  alignas(64) Slot buf_[N];
  alignas(64) std::atomic<size_t> tail_{0};
  alignas(64) std::atomic<size_t> head_{0};

public:
  MPSCQueue() {
    for (size_t i = 0; i < N; ++i)
      buf_[i].seq.store(i, std::memory_order_relaxed);
  }

  // Called by multiple producers
  bool push(const T& val) {
    size_t tail = tail_.load(std::memory_order_relaxed);
    for (;;) {
      Slot& slot = buf_[tail & (N-1)];
      size_t seq = slot.seq.load(std::memory_order_acquire);
      intptr_t diff = (intptr_t)seq - (intptr_t)tail;
      if (diff == 0) {
        if (tail_.compare_exchange_weak(tail, tail+1, std::memory_order_relaxed))
          break;
      } else if (diff < 0) {
        return false; // full
      } else {
        tail = tail_.load(std::memory_order_relaxed);
      }
    }
    buf_[tail & (N-1)].val = val;
    buf_[tail & (N-1)].seq.store(tail+1, std::memory_order_release);
    return true;
  }

  // Called by single consumer only
  bool pop(T& val) {
    size_t head = head_.load(std::memory_order_relaxed);
    Slot& slot = buf_[head & (N-1)];
    size_t seq = slot.seq.load(std::memory_order_acquire);
    intptr_t diff = (intptr_t)seq - (intptr_t)(head+1);
    if (diff < 0) return false; // empty
    val = slot.val;
    slot.seq.store(head + N, std::memory_order_release);
    head_.store(head+1, std::memory_order_relaxed);
    return true;
  }
};