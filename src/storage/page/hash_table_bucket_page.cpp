//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

#define BYTE_IDX(i) ((i) >> 3)
#define BIT_MASK(i) (1 << ((i) & 7))
#define BIT_UNMASK(i) (BIT_MASK(i) ^ 0xff)

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
  for (uint32_t bucket_idx = 0; bucket_idx < size_; bucket_idx++) {
    if (IsReadable(bucket_idx)) {
      if (cmp(KeyAt(bucket_idx), key)) {
        result->push_back(ValueAt(bucket_idx));
      }
    }
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
  uint32_t earliest_vacant = size_;
  for (uint32_t bucket_idx = 0; bucket_idx < size_; bucket_idx++) {
    if (IsReadable(bucket_idx)) {
      // Duplicate key-value pair
      if (cmp(KeyAt(bucket_idx), key) && ValueAt(bucket_idx) == value) {
        return false;
      }
    } else {
      // A vacant slot is found
      if (earliest_vacant == size_) {
        earliest_vacant = bucket_idx;
      }
    }
  }
  if (earliest_vacant == size_) {
    SetOccupied(earliest_vacant);
    size_++;
  } else {
    free_--;
  }
  SetReadable(earliest_vacant);
  taken_++;
  array_[earliest_vacant] = std::make_pair(key, value);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  for (uint32_t bucket_idx = 0; bucket_idx < size_; bucket_idx++) {
    if (IsReadable(bucket_idx)) {
      // The key-value pair is found
      if (cmp(KeyAt(bucket_idx), key) && ValueAt(bucket_idx) == value) {
        RemoveAt(bucket_idx);
        return true;
      }
    }
  }
  // Key-value pair not found
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  taken_--;
  free_++;
  SetUnreadable(bucket_idx);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
  return (occupied_[BYTE_IDX(bucket_idx)] & BIT_MASK(bucket_idx)) == 1;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  return (readable_[BYTE_IDX(bucket_idx)] & BIT_MASK(bucket_idx)) == 1;
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetUnreadable(uint32_t bucket_idx) {
  readable_[BYTE_IDX(bucket_idx)] &= BIT_UNMASK(bucket_idx);
}

template <typename KeyType, typename ValueType, typename KeyComparator>

bool HASH_TABLE_BUCKET_TYPE::IsFull() {
  return taken_ == BUCKET_ARRAY_SIZE;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
  return taken_;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
  return taken_ == 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size_, taken_, free_);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
