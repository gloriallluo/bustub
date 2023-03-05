//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
  pin_count_.reserve(num_pages + 1);
  pin_count_.assign(num_pages + 1, -1); // no in-memory page
}

LRUReplacer::~LRUReplacer() = default;

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
  if (candidates_.empty()) {
    return false; // no evictable page
  }
  latch_.lock();
  *frame_id = candidates_.front();
  candidates_.pop_front();
  latch_.unlock();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  latch_.lock();
  // if the frame is in memory
  if (pin_count_[frame_id] >= 0) {
    // remove it from candidates_
    if (pin_count_[frame_id] == 0) {
      auto iter = std::find(candidates_.begin(), candidates_.end(), frame_id);
      if (iter != candidates_.end()) {
        candidates_.erase(iter);
      }
    }
    // update pin_count_
    pin_count_[frame_id]++;
  }
  latch_.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  latch_.lock();
  // if the frame is pinned or not in memory
  if (pin_count_[frame_id] != 0) {
    pin_count_[frame_id] = 0;
    candidates_.push_back(frame_id);
  }
  latch_.unlock();
}

auto LRUReplacer::Size() -> size_t { return candidates_.size(); }

}  // namespace bustub
