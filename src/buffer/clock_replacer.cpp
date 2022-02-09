//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
  clock_hand_ = 1;
  unpinned_counter_ = 0;
  num_pages_ = num_pages;
  state_.reserve(num_pages + 1);
  state_.assign(num_pages + 1, Out);
}

ClockReplacer::~ClockReplacer() = default;

auto ClockReplacer::Victim(frame_id_t *frame_id) -> bool { return false; }

void ClockReplacer::Pin(frame_id_t frame_id) {
  if (state_[frame_id] == Ready || state_[frame_id] == Referenced) {
    unpinned_counter_ --;
  }
  state_[frame_id] = Pinned;
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  if (state_[frame_id] == Out) {
    unpinned_counter_ ++;
    state_[frame_id] = Ready;
  }
  if (state_[frame_id] == Pinned) {
    unpinned_counter_ ++;
    state_[frame_id] = Referenced;
  }
}

auto ClockReplacer::Size() -> size_t { return 0; }

}  // namespace bustub
