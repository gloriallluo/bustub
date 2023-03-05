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
  // allocate a frame in the buffer pool
  frame_id_t frame_id;
  if (!AllocateNewFrame(&frame_id)) {
    return nullptr;
  }

  // allocate a page_id
  *page_id = AllocatePage();

  // update the frame's metadata
  Page *page = &pages_[frame_id % pool_size_];
  page->WLatch();
  page->ResetMemory();
  page->page_id_ = *page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;
  page->WUnlatch();

  // pin this page
  replacer_->RecordAccess(frame_id, AccessType::Get);
  replacer_->SetEvictable(frame_id, false);
  // add to the page table
  page_table_.emplace(std::make_pair(*page_id, frame_id));
  return page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, AccessType access_type) -> Page * {
  auto iter = page_table_.find(page_id);

  // the page is in the buffer pool
  if (iter != page_table_.end()) {
    replacer_->RecordAccess(iter->second, access_type);
    replacer_->SetEvictable(iter->second, false);

    // pin it and return the pointer.
    Page *page = &pages_[iter->second % pool_size_];
    page->WLatch();
    page->pin_count_++;
    page->WUnlatch();
    return page;
  }

  // the page is not in the buffer pool, allocate a new frame for it
  frame_id_t frame_id;
  if (!AllocateNewFrame(&frame_id)) {
    return nullptr;
  }

  // update metadata
  Page *page = &pages_[frame_id % pool_size_];
  page->WLatch();
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;
  disk_manager_->ReadPage(page_id, page->GetData());
  page->WUnlatch();

  // pin this page
  replacer_->RecordAccess(frame_id, access_type);
  replacer_->SetEvictable(frame_id, false);
  // add to the page table
  page_table_.emplace(std::make_pair(page_id, frame_id));
  return page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, AccessType access_type) -> bool {
  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id % pool_size_];
  page->RLatch();
  if (page->GetPinCount() <= 0) {
    page->RUnlatch();
    return false;
  }
  page->RUnlatch();

  // update metadata
  page->WLatch();
  page->is_dirty_ = is_dirty;
  page->WUnlatch();

  // unpin the frame
  replacer_->RecordAccess(frame_id, access_type);
  replacer_->SetEvictable(frame_id, true);
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  auto iter = page_table_.find(page_id);
  // the page is not in-memory
  if (iter == page_table_.end()) {
    return false;
  }
  Page *page = &pages_[iter->second % pool_size_];

  // flush to disk
  page->WLatch();
  page->is_dirty_ = false;
  disk_manager_->WritePage(page_id, page->GetData());
  page->WUnlatch();
  return true;
}

void BufferPoolManager::FlushAllPages() {
  for (auto p : page_table_) {
    FlushPage(p.first);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  DeallocatePage(page_id);
  auto iter = page_table_.find(page_id);

  // the page is not in-memory, return true
  if (iter == page_table_.end()) {
    return true;
  }

  Page *page = &pages_[iter->second % pool_size_];
  page->RLatch();
  // someone is using the page, return false
  if (page->GetPinCount() > 0) {
    page->RUnlatch();
    return false;
  }
  page->RUnlatch();

  // add it to the free list
  free_list_.push_back(iter->second);

  // reset metadata
  page->WLatch();
  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;
  page->is_dirty_ = false;
  page->WUnlatch();

  // remove from page table
  page_table_.erase(iter);
  return false;
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
  if (!replacer_->Evict(frame_id)) {
    printf("Got nothing to evict from replacer\n");
    return false;
  }
  // frame_id is to be evicted
  for (auto iter = page_table_.begin(); iter != page_table_.end(); ++iter) {
    // find the corresponding page_id
    if (iter->second == *frame_id) {
      FlushPage(iter->first);
      page_table_.erase(iter);
      break;
    }
  }
  return true;
}

}  // namespace bustub
