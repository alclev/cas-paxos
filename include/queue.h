#pragma once

#include <atomic>
#include <cstddef>
#include <utility>


// Single-producer, single-consumer bounded ring buffer.
//
// One thread may call enqueue(), one (different) thread may call dequeue().
// No other combination is safe.
//
template <typename T, std::size_t Capacity>
class ReaderWriterQueue {
  static constexpr std::size_t kCacheLine = 64;
  static constexpr std::size_t kMask = Capacity - 1;

  static_assert(Capacity >= 2 && (Capacity & kMask) == 0,
                "Capacity must be a power of two >= 2");

 public:
  ReaderWriterQueue() = default;

  ReaderWriterQueue(ReaderWriterQueue const &) = delete;
  ReaderWriterQueue &operator=(ReaderWriterQueue const &) = delete;

  // Producer thread only. Returns false if the queue is full.
  template <typename U>
  bool enqueue(U &&v) {
    std::size_t const tail = tail_.load(std::memory_order_relaxed);
    std::size_t const next = (tail + 1) & kMask;

    if (next == cached_head_) {
      // Might be full; refresh our view of the consumer's index.
      cached_head_ = head_.load(std::memory_order_acquire);
      if (next == cached_head_) {
        return false;
      }
    }

    slots_[tail] = std::forward<U>(v);
    tail_.store(next, std::memory_order_release);
    return true;
  }

  // Consumer thread only. Returns false if the queue is empty.
  bool dequeue(T &out) {
    std::size_t const head = head_.load(std::memory_order_relaxed);

    if (head == cached_tail_) {
      // Might be empty; refresh our view of the producer's index.
      cached_tail_ = tail_.load(std::memory_order_acquire);
      if (head == cached_tail_) {
        return false;
      }
    }

    out = std::move(slots_[head]);
    head_.store((head + 1) & kMask, std::memory_order_release);
    return true;
  }

  // Safe from either thread, but only a lower bound under concurrency.
  std::size_t size_approx() const {
    std::size_t const tail = tail_.load(std::memory_order_acquire);
    std::size_t const head = head_.load(std::memory_order_acquire);
    return (tail - head) & kMask;
  }

  static constexpr std::size_t capacity() { return Capacity - 1; }

 private:
  T slots_[Capacity];

  // Producer's line: it owns tail_ and cached_head_.
  alignas(kCacheLine) std::atomic<std::size_t> tail_{0};
  std::size_t cached_head_{0};

  // Consumer's line: it owns head_ and cached_tail_.
  alignas(kCacheLine) std::atomic<std::size_t> head_{0};
  std::size_t cached_tail_{0};
};
