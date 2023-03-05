//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
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

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
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

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
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

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool { return false; }

void BufferPoolManager::FlushAllPages() {}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
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

auto BufferPoolManager::AllocatePage() -> page_id_t {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += static_cast<page_id_t>(num_instances_);
  ValidatePageId(next_page_id);
  return next_page_id;
}

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

bool BufferPoolManager::AllocateNewFrame(frame_id_t *frame_id) {
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
      FlushPage(iter->first);
      page_table_.erase(iter);
      break;
    }
  }
  return true;
}

}  // namespace bustub
