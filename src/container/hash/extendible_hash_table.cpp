//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
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
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
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
inline auto HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  return Hash(key) & dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  return static_cast<uint32_t>(dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page)));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchDirectoryPage() -> HashTableDirectoryPage * {
  if (directory_page_id_ == INVALID_PAGE_ID) {
    page_lock_.lock();
    if (directory_page_id_ == INVALID_PAGE_ID) {
      // renew
      LOG_DEBUG("create new directory, before %d", directory_page_id_);
      Page *page = buffer_pool_manager_->NewPage(&directory_page_id_);
      auto dir_page = reinterpret_cast<HashTableDirectoryPage *>(page->GetData());
      assert(directory_page_id_ != INVALID_PAGE_ID);
      page_id_t bucket_page_id;

      auto bucket_page = buffer_pool_manager_->NewPage(&bucket_page_id);
      assert(bucket_page != nullptr);
      dir_page->SetBucketPageId(0, bucket_page_id);

      assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
    }
    page_lock_.unlock();
  }

  assert(directory_page_id_ != INVALID_PAGE_ID);

  return reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(directory_page_id_)->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) -> Page * {
  return buffer_pool_manager_->FetchPage(bucket_page_id);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::ToBucketPage(Page *page) -> HASH_TABLE_BUCKET_TYPE * {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) -> bool {
  table_latch_.RLock();
  auto dir_page = FetchDirectoryPage();
  auto page_id = KeyToPageId(key, dir_page);
  auto page = FetchBucketPage(page_id);

  page->RLatch();
  auto bucket_page = ToBucketPage(page);
  auto res = bucket_page->GetValue(key, comparator_, result);
  page->RUnlatch();

  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
  assert(buffer_pool_manager_->UnpinPage(page_id, false));

  table_latch_.RUnlock();
  return res;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.RLock();

  auto dir_page = FetchDirectoryPage();
  auto page_id = KeyToPageId(key, dir_page);
  auto page = FetchBucketPage(page_id);

  page->WLatch();
  auto bucket_page = ToBucketPage(page);

  if (!bucket_page->IsFull()) {
    auto res = bucket_page->Insert(key, value, comparator_);
    page->WUnlatch();
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
    assert(buffer_pool_manager_->UnpinPage(page_id, true));
    table_latch_.RUnlock();
    return res;
  }

  page->WUnlatch();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
  assert(buffer_pool_manager_->UnpinPage(page_id, false));
  table_latch_.RUnlock();
  return SplitInsert(transaction, key, value);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.WLock();

  auto dir_page = FetchDirectoryPage();
  auto global_depth = dir_page->GetGlobalDepth();
  auto bucket_idx = KeyToDirectoryIndex(key, dir_page);
  auto local_depth = dir_page->GetLocalDepth(bucket_idx);

  if (local_depth >= 9) {
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
    table_latch_.WUnlock();
    return false;
  }

  auto origin_page_id = KeyToPageId(key, dir_page);
  auto origin_page = FetchBucketPage(origin_page_id);
  auto origin_bucket_page = ToBucketPage(origin_page);

  dir_page->IncrLocalDepth(bucket_idx);
  local_depth++;
  if (local_depth > global_depth) {
    // update global depth
    dir_page->IncrGlobalDepth();
  }

  auto split_bucket_id = dir_page->GetSplitImageIndex(bucket_idx);
  page_id_t split_page_id;
  auto split_page = buffer_pool_manager_->NewPage(&split_page_id);
  auto split_bucket_page = ToBucketPage(split_page);
  dir_page->SetBucketPageId(split_bucket_id, split_page_id);

  uint32_t diff = 1 << dir_page->GetLocalDepth(bucket_idx);
  for (uint32_t i = bucket_idx; i >= diff; i -= diff) {
    dir_page->SetBucketPageId(i, origin_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(bucket_idx));
  }
  for (uint32_t i = bucket_idx; i < dir_page->Size(); i += diff) {
    dir_page->SetBucketPageId(i, origin_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(bucket_idx));
  }

  for (uint32_t i = split_bucket_id; i >= diff; i -= diff) {
    dir_page->SetBucketPageId(i, split_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(bucket_idx));
  }
  for (uint32_t i = split_bucket_id; i < dir_page->Size(); i += diff) {
    dir_page->SetBucketPageId(i, split_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(bucket_idx));
  }

  origin_page->WLatch();
  split_page->WLatch();
  auto num = origin_bucket_page->NumReadable();
  auto array = origin_bucket_page->GetArrayCopy();
  origin_bucket_page->Clear();
  for (uint32_t i = 0; i < num; i++) {
    auto old_key = array[i].first;
    auto old_value = array[i].second;
    auto new_bucket_id = KeyToDirectoryIndex(old_key, dir_page);
    if (new_bucket_id == bucket_idx) {
      assert(origin_bucket_page->Insert(old_key, old_value, comparator_));
    } else {
      assert(split_bucket_page->Insert(old_key, old_value, comparator_));
    }
  }
  split_page->WUnlatch();
  origin_page->WUnlatch();

  assert(buffer_pool_manager_->UnpinPage(origin_page_id, true));
  assert(buffer_pool_manager_->UnpinPage(split_page_id, true));
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true));
  table_latch_.WUnlock();
  return Insert(transaction, key, value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.RLock();
  auto dir_page = FetchDirectoryPage();
  auto page_id = KeyToPageId(key, dir_page);
  auto bucket_id = KeyToDirectoryIndex(key, dir_page);
  auto page = FetchBucketPage(page_id);
  auto bucket_page = ToBucketPage(page);

  assert(page != nullptr);
  page->WLatch();
  auto res = bucket_page->Remove(key, value, comparator_);
  if (!res) {
    page->WUnlatch();
    table_latch_.RUnlock();
    assert(buffer_pool_manager_->UnpinPage(page_id, false));
  } else if (bucket_page->IsEmpty()) {
    auto dir_page = FetchDirectoryPage();
    auto local_depth = dir_page->GetLocalDepth(bucket_id);
    if (dir_page->GetLocalDepth(bucket_id) != 0 &&
        dir_page->GetLocalDepth(dir_page->GetSplitImageIndex(bucket_id)) == local_depth) {
      assert(buffer_pool_manager_->UnpinPage(page_id, true));
      page->WUnlatch();
      table_latch_.RUnlock();
      Merge(transaction, key, value);
    } else {
      assert(buffer_pool_manager_->UnpinPage(page_id, true));
      page->WUnlatch();
      table_latch_.RUnlock();
    }
  } else {
    assert(buffer_pool_manager_->UnpinPage(page_id, true));
    page->WUnlatch();
    table_latch_.RUnlock();
  }

  return res;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  auto dir_page = FetchDirectoryPage();
  auto cur_page_id = KeyToPageId(key, dir_page);
  auto cur_page = FetchBucketPage(cur_page_id);
  auto cur_bucket_page = ToBucketPage(cur_page);
  auto cur_bucket_idx = KeyToDirectoryIndex(key, dir_page);
  auto split_bucket_idx = dir_page->GetSplitImageIndex(cur_bucket_idx);

  int cur_depth = dir_page->GetLocalDepth(cur_bucket_idx);
  uint32_t cur_mask = cur_bucket_idx & ((1 << dir_page->GetLocalDepth(cur_bucket_idx)) - 1);
  uint32_t split_mask = split_bucket_idx & ((1 << dir_page->GetLocalDepth(split_bucket_idx)) - 1);

  if (cur_bucket_idx > dir_page->Size()) {
    table_latch_.WUnlock();
    return;
  }

  cur_page->RLatch();
  if (!cur_bucket_page->IsEmpty()) {
    // bucket is not empty
    cur_page->RUnlatch();
    assert(buffer_pool_manager_->UnpinPage(cur_page_id, false));
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
    table_latch_.WUnlock();
    return;
  }
  assert(buffer_pool_manager_->UnpinPage(cur_page_id, false));
  cur_page->RUnlatch();

  auto size = dir_page->Size();
  for (uint32_t i = 0; i < size; i++) {
    if ((i & cur_mask) == cur_mask && (i | cur_mask) == cur_mask) {
      dir_page->SetLocalDepth(i, cur_depth - 1);
      dir_page->SetBucketPageId(i, dir_page->GetBucketPageId(split_bucket_idx));
    } else if ((i & split_mask) == split_mask && (i | split_mask) == split_mask) {
      dir_page->SetLocalDepth(i, cur_depth - 1);
    }
  }

  while (dir_page->CanShrink()) {
    dir_page->DecrGlobalDepth();
  }

  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true));
  table_latch_.WUnlock();
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetGlobalDepth() -> uint32_t {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
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
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
