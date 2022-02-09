//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(static_cast<page_id_t>(instance_index)),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }
  Page *page = &pages_[iter->second % pool_size_];
  if (page->IsDirty()) {
    disk_manager_->WritePage(page_id, page->GetData());
  }
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  for (auto &pf : page_table_) {
    Page *page = &pages_[pf.second % pool_size_];
    if (page->IsDirty()) {
      disk_manager_->WritePage(pf.first, page->GetData());
    }
  }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // Allocate a page from disk.
  *page_id = AllocatePage();

  // Allocate a frame in the buffer pool.
  frame_id_t frame_id;
  if (!AllocateNewFrame(&frame_id)) {
    return nullptr;
  }

  // Update the frame's metadata.
  Page *page = &pages_[frame_id % pool_size_];
  page->WLatch();
  page->ResetMemory();
  page->page_id_ = *page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;
  page->WUnlatch();

  // Pin this page.
  replacer_->Pin(frame_id);
  // Add P to the page table.
  page_table_.emplace(std::make_pair(*page_id, frame_id));
  return page;
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  auto iter = page_table_.find(page_id);
  if (iter != page_table_.end()) {
    // The page is in the buffer pool, pin it and return the pointer.
    replacer_->Pin(iter->second);
    Page *page = &pages_[iter->second % pool_size_];
    page->WLatch();
    page->pin_count_++;
    page->WUnlatch();
    return page;
  }

  // The page is not in the buffer pool, allocate a new frame for it.
  frame_id_t frame_id;
  if (!AllocateNewFrame(&frame_id)) {
    return nullptr;
  }

  // Update metadata.
  Page *page = &pages_[frame_id % pool_size_];
  page->WLatch();
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;
  disk_manager_->ReadPage(page_id, page->GetData());
  page->WUnlatch();

  // Pin this page.
  replacer_->Pin(frame_id);
  // Add P to the page table.
  page_table_.emplace(std::make_pair(page_id, frame_id));
  return page;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  DeallocatePage(page_id);
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    // The page is not in-memory, return true.
    return true;
  }
  Page *page = &pages_[iter->second % pool_size_];
  page->RLatch();
  // Someone is using the page, return false.
  if (page->GetPinCount() > 0) {
    page->RUnlatch();
    return false;
  }
  page->RUnlatch();

  // Add it to the free list.
  free_list_.push_back(iter->second);
  // Reset P's metadata.
  page->WLatch();
  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;
  page->is_dirty_ = false;
  page->WUnlatch();
  // Remove P from the page table.
  page_table_.erase(iter);
  return false;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id % pool_size_];
  page->RLatch();
  if (page->GetPinCount() <= 0) {
    page->RUnlatch();
    return false;
  }
  page->RUnlatch();

  // Update metadata.
  page->WLatch();
  page->is_dirty_ = is_dirty;
  page->WUnlatch();
  replacer_->Unpin(frame_id);
  return true;
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += static_cast<page_id_t>(num_instances_);
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

bool BufferPoolManagerInstance::AllocateNewFrame(frame_id_t *frame_id) {
  if (!free_list_.empty()) {
    *frame_id = free_list_.front();
    free_list_.pop_front();
    return true;
  }
  if (!replacer_->Victim(frame_id)) {
    return false;
  }
  for (auto iter = page_table_.begin(); iter != page_table_.end(); ++iter) {
    if (iter->second == *frame_id) {
      FlushPgImp(iter->first);
      page_table_.erase(iter);
      break;
    }
  }
  return true;
}
}  // namespace bustub
