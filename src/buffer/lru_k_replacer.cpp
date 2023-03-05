//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : k_(k), replacer_size_(0) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  latch_.lock();

  // check candidates_ first
  for (frame_id_t fid : candidates_) {
    if (node_store_[fid].is_evictable_) {
      *frame_id = fid;
      candidates_.remove(fid);
      node_store_.erase(node_store_.find(fid));
      replacer_size_ --;
      latch_.unlock();
      return true;
    }
  }

  // check candidates_k_
  for (frame_id_t fid : candidates_k_) {
    if (node_store_[fid].is_evictable_) {
      *frame_id = fid;
      candidates_k_.remove(fid);
      node_store_.erase(node_store_.find(fid));
      replacer_size_ --;
      latch_.unlock();
      return true;
    }
  }
  latch_.unlock();
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  latch_.lock();
  
  // the first access
  bool first_access = node_store_.find(frame_id) == node_store_.end();
  if (first_access) {
    node_store_[frame_id] = LRUKNode {};
    candidates_.push_back(frame_id);
    replacer_size_ ++;
  }
  
  // update access count
  node_store_[frame_id].access_cnt += 1;
  
  if (!first_access) {
    candidates_.remove(frame_id);
    candidates_k_.remove(frame_id);
    if (node_store_[frame_id].access_cnt >= k_) {
      candidates_k_.push_back(frame_id);
    } else {
      candidates_.push_back(frame_id);
    }
  }

  latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  latch_.lock();
  bool prev_is_evictable = node_store_[frame_id].is_evictable_;
  if (prev_is_evictable != set_evictable) {
    replacer_size_ += (set_evictable ? 1 : -1);
  }
  node_store_[frame_id].is_evictable_ = set_evictable;
  latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  latch_.lock();
  // XXX I don't know what it's for
  node_store_[frame_id].access_cnt = 0;
  latch_.unlock();
}

auto LRUKReplacer::Size() -> size_t { return replacer_size_; }

}  // namespace bustub
