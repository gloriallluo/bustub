//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/disk/hash/disk_extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                         const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  page_id_t directory_page_id;
  HashTableDirectoryPage *directory_page = reinterpret_cast<HashTableDirectoryPage*>(
      buffer_pool_manager_->NewPage(&directory_page_id)->GetData());
  directory_page_id_ = directory_page_id;
  directory_page->SetPageId(directory_page_id_);

  // The depth of the directory page is initialized to 1
  directory_page->IncrGlobalDepth();
  page_id_t bucket_page_id;
  HASH_TABLE_BUCKET_TYPE *page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE*>(
      buffer_pool_manager_->NewPage(&bucket_page_id)->GetData());
  directory_page->SetLocalDepth(0, directory_page->GetGlobalDepth());
  directory_page->SetBucketPageId(0, bucket_page_id);
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Hash(KeyType key) -> uint32_t {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  return Hash(key) & dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  return dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  Page *page = buffer_pool_manager_->FetchPage(directory_page_id_);
  return reinterpret_cast<HashTableDirectoryPage*>(page->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  Page *page = buffer_pool_manager_->FetchPage(bucket_page_id);
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE*>(page->GetData());
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(KeyToPageId(key, dir_page));
  return bucket_page->GetValue(key, comparator_, result);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  return false;
}


template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  bool res = bucket_page->Insert(key, value, comparator_);

  // The bucket being inserted is full, then split
  if (bucket_page->IsFull()) {
    uint32_t prev_depth = dir_page->GetGlobalDepth();
    uint32_t prev_mask = dir_page->GetGlobalDepthMask();
    uint32_t split_bucket_id = KeyToDirectoryIndex(key, dir_page);

    // Increment global depth
    dir_page->IncrGlobalDepth();

    // Double bucket entries
    for (uint32_t i = prev_depth; i < 2 * prev_depth; i++) {
      // The `local_depth` and `bucket_page_id` of 1xxx are both set to that of 0xxx
      dir_page->SetBucketPageId(i, dir_page->GetBucketPageId(i & prev_mask));
      dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(i & prev_mask));
    }

    // Allocate a new page for the split bucket
    uint32_t new_bucket_id = (split_bucket_id | (~prev_mask)) & dir_page->GetGlobalDepthMask();
    page_id_t new_page_id;
    HASH_TABLE_BUCKET_TYPE *new_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE*>(
        buffer_pool_manager_->NewPage(&new_page_id));
    dir_page->SetBucketPageId(new_bucket_id, new_page_id);
    dir_page->SetLocalDepth(new_bucket_id, dir_page->GetLocalDepth(split_bucket_id) - 1);

    // Move some KVs to the new page
    for (uint32_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
      if (bucket_page->IsReadable(bucket_idx) &&
          (Hash(bucket_page->KeyAt(bucket_idx)) & dir_page->GetLocalHighBit(split_bucket_id)) == 1) {
        new_page->Insert(bucket_page->KeyAt(bucket_idx), bucket_page->ValueAt(bucket_idx), comparator_);
        bucket_page->RemoveAt(bucket_idx);
      }
    }
  }
  return res;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(KeyToPageId(key, dir_page));
  bool res = bucket_page->Remove(key, value, comparator_);
  if (bucket_page->IsEmpty()) {
    Merge(transaction, key, value);
  }
  return res;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // TODO
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetGlobalDepth() -> uint32_t {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class DiskExtendibleHashTable<int, int, IntComparator>;

template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
