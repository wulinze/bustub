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
  std::lock_guard<std::mutex> guard(page_lock);
  return reinterpret_cast<HashTableDirectoryPage *> buffer_pool_manager_->FetchPage(directory_page_id_);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) -> HASH_TABLE_BUCKET_TYPE * {
  std::lock_guard<std::mutex> guard(page_lock);
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE*> buffer_pool_manager_->FetchPage(bucket_page_id);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) -> bool {
  auto bucket_page = FetchBucketPage(KeyToPageId(key));

  return bucket_page->GetValue(key, result);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.RLock();

  auto bucket_page = FetchBucketPage(KeyToPageId(key));
  if (!bucket_page->Insert(key, value, comparator_)) {
    table_latch_.RUnlock();
    return SplitInsert(transaction, key, value);
  }

  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.WLock();

  auto dir_page = FetchDirectoryPage();
  auto global_depth = dir_page->GetGlobalDepth();
  auto bucket_idx = KeyToDirectoryIndex(key, dir_page);
  auto origin_page_id = KeyToPageId(key);
  auto origin_page = FetchBucketPage(KeyToPageId(key));
  auto local_depth = dir_page->GetLocalDepth(bucket_idx);

  if(local_depth >= 9){
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    table_latch_.WUnlock();
    return false;
  }

  dir_page->IncrLocalDepth(bucket_idx);
  if (local_depth > global_depth) {
    // update global depth
    dir_page->IncrGlobalDepth();
  } 

  auto split_bucket_id = dir_page->GetSplitImageIndex(bucket_idx);
  page_id_t split_page_id;
  auto split_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE*>(buffer_pool_manager_->NewPage(split_page_id)); 
  dir_page->SetBucketPageId(split_bucket_id, split_page_id);  

  uint32_t diff = 1 << dir_page->GetLocalDepth(split_bucket_id);
  for(int i=split_bucket_id; i >= diff; i-=diff){
    dir_page->SetBucketPageId(i, split_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_bucket_id));
  }
  for(int i=split_bucket_id; i < dir_page->Size(); i+=diff){
    dir_page->SetBucketPageId(i, split_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_bucket_id));
  }

  for(int i=bucket_idx; i >= diff; i-=diff){
    dir_page->SetBucketPageId(i, origin_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(bucket_idx));
  }
  for(int i=bucket_idx; i < dir_page->Size(); i+=diff){
    dir_page->SetBucketPageId(i, origin_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(bucket_idx));
  }


  for (int i=0; i < origin_page->NumReadable(); i++) {
    auto old_key = origin_page->KeyAt(i);
    auto old_value = origin_page->Value(i);
    auto newBucketId = KeyToDirectoryIndex(key);
    if (newBucketId != bucket_idx) {
      origin_page->RemoveAt(i);
      split_page->Insert(old_key, old_value, comparator_);
    }
  }

  assert(buffer_pool_manager_->UnpinPage(bucket_idx, true));
  assert(buffer_pool_manager_->UnpinPage(split_bucket_id, true));
  table_latch_.WUnlock();
  return Insert(transaction, key, value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.RLock();
  auto bucket_id = KeyToPageId(key);
  auto bucket_page = FetchBucketPage(bucket_id);

  if (!bucket_page->Remove(key, value, comparator_)) {
    buffer_pool_manager_->UnpinPage(bucket_id, false);
    return false;
  } else if (bucket_page->isEmpty()) {
    auto dir_page = FetchDirectoryPage();
    auto local_depth = dir_page->GetLocalDepth(bucket_id);
    table_latch_.RUnlock();
    if (dir_page->GetLocalDepth(bucket_id) != 0 && 
    dir_page->GetLocalDepth(dir_page->GetSplitImageIndex()) == local_depth) {
      buffer_pool_manager_->UnpinPage(bucket_id, true);
      Merge(transaction, key, value);
    }
  }

  table_latch_.RUnlock();
  return true;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  auto dir_page = FetchDirectoryPage();
  auto cur_bucket_idx = KeyToDirectoryIndex(key);
  auto split_bucket_idx = dir_page->GetSplitImageIndex(cur_bucket_idx);

  int cur_depth = dir_page->GetLocalDepth(cur_bucket_idx);
  char cur_mask = cur_bucket_idx & ((1 << dir_page->GetLocalDepth(cur_bucket_idx)) - 1);
  char split_mask = split_bucket_idx & ((1 << dir_page->GetLocalDepth(split_bucket_idx)) - 1);

  if(cur_bucket_idx > dir_page->Size()){
    table_latch_.WUnlock();
    return;
  }

  for(uint32_t i=0; i < dir_page->Size(); i++){
    if (i & cur_mask == cur_mask && i | cur_mask == i) {
      dir_page->SetLocalDepth(cur_depth-1);
    } else if (i & cur_mask == cur_mask && i | cur_mask == i) {
      dir_page->SetLocalDepth(cur_depth-1);
      dir_page->SetBucketPageId(i, dir_page->GetBucketPageId(split_bucket_idx));
    }
  }

  while (dir_page->CanShrink()) {
    dir_page->DecrGlobalDepth();
  }

  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true));
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
