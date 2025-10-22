#pragma once

#include <cinttypes>
#include <cstdlib>
#include <memory>

#include "common.h"

constexpr bool is_pow2(uint32_t v) { return v && ((v & (v - 1)) == 0); }

namespace romulus {

template <typename T, uint32_t SlotSize, uint32_t Alignment>
class APArray {
  static_assert(sizeof(T) <= SlotSize);
  static_assert(is_pow2(Alignment) == is_pow2(Alignment));

 public:
  APArray() : capacity_(0), raw_(nullptr) {}
  APArray(uint32_t capacity) : capacity_(capacity) {
    bool is_multiple = (capacity_ * SlotSize) % Alignment == 0;
    bytes_ =
        is_multiple
            ? (static_cast<uint64_t>(capacity_) * SlotSize)
            : (((static_cast<uint64_t>(capacity_) * SlotSize) / Alignment) +
               1) *
                  Alignment;
    ROMULUS_INFO(
        "Allocating aligned memory: alignment={}, slotsize={}, capacity={}, "
        "bytes={}, is_multiple={}",
        Alignment, SlotSize, capacity_, bytes_, is_multiple);
    raw_ = reinterpret_cast<uint8_t*>(std::aligned_alloc(Alignment, bytes_));
    ROMULUS_ASSERT(raw_ != nullptr, "aligned_alloc failed: error={}", std::strerror(errno));
  }
  ~APArray() { free(raw_); }
  T& operator[](uint32_t index) {
    return reinterpret_cast<T&>(raw_[index * SlotSize]);
  }
  uint32_t GetCapacity() const { return capacity_; }
  uint32_t GetSlotSize() const { return SlotSize; }
  uint64_t GetTotalBytes() const { return bytes_; }
  uint8_t* Get() const { return raw_; }

 private:
  uint32_t capacity_;
  uint64_t bytes_;
  uint8_t* raw_;
};

template <typename T, uint32_t SlotSize, uint32_t Alignment>
class APArraySlice {
 public:
  APArraySlice() : raw_(nullptr), start_(0), end_(0) {}
  APArraySlice(const APArray<T, SlotSize, Alignment>* const base,
               uint32_t start, uint32_t end)
      : raw_(base->Get()), start_(start), end_(end) {
    ROMULUS_ASSERT(start_ < end_,
                  "Invalid arguments: start ({}) >= end ({})", start_, end_);
  }
  T& operator[](uint32_t index) {
    ROMULUS_ASSERT(start_ + index < end_,
                  "Index out of bounds: start ({}) + index ({}) >= end ({})",
                  start_, index, end_);
    return reinterpret_cast<T&>(raw_[(start_ * SlotSize) + (index * SlotSize)]);
  }

  uint8_t* Begin() const { return raw_ + (start_ * SlotSize); }
  uint32_t Size() const { return end_ - start_; }

 private:
  uint8_t* raw_;
  uint32_t start_;
  uint32_t end_;
};

}  // namespace romulus