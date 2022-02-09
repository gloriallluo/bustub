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
  pin_count_.assign(num_pages + 1, -1);
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if (candidates_.empty()) {
    return false;
  }
  *frame_id = candidates_.front();
  candidates_.pop_front();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  // if the frame is in memory
  if (pin_count_[frame_id] >= 0) {
    // remove it from the replacer
    if (pin_count_[frame_id] == 0) {
      auto iter = std::find(candidates_.begin(), candidates_.end(), frame_id);
      if (iter != candidates_.end()) {
        candidates_.erase(iter);
      }
    }
    pin_count_[frame_id]++;
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  // if the frame is pinned or not in memory
  if (pin_count_[frame_id] != 0) {
    pin_count_[frame_id] = 0;
    candidates_.push_back(frame_id);
  }
}

size_t LRUReplacer::Size() { return candidates_.size(); }

}  // namespace bustub
