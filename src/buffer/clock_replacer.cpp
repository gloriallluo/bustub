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
  state_.assign(num_pages + 1, ClockState::OUT);
}

ClockReplacer::~ClockReplacer() = default;

auto ClockReplacer::Victim(frame_id_t *frame_id) -> bool {
  if (unpinned_counter_ == 0) {
    return false;
  }
  while (state_[clock_hand_] != ClockState::READY) {
    if (state_[clock_hand_] == ClockState::REFERENCED) {
      state_[clock_hand_] = ClockState::READY;
    }
    UpdateClockHand();
  }
  *frame_id = clock_hand_;
  state_[clock_hand_] = ClockState::OUT;
  unpinned_counter_--;
  UpdateClockHand();
  return true;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  if (state_[frame_id] == ClockState::READY || state_[frame_id] == ClockState::REFERENCED) {
    unpinned_counter_--;
  }
  state_[frame_id] = ClockState::PINNED;
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  if (state_[frame_id] == ClockState::OUT) {
    unpinned_counter_++;
    state_[frame_id] = ClockState::READY;
  }
  if (state_[frame_id] == ClockState::PINNED) {
    unpinned_counter_++;
    state_[frame_id] = ClockState::REFERENCED;
  }
}

auto ClockReplacer::Size() -> size_t { return unpinned_counter_; }

void ClockReplacer::UpdateClockHand() {
  clock_hand_++;
  if (clock_hand_ > static_cast<frame_id_t>(num_pages_)) {
    clock_hand_ = 1;
  }
}

}  // namespace bustub
