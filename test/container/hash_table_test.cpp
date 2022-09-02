//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_test.cpp
//
// Identification: test/container/hash_table_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <thread>  // NOLINT
#include <vector>

#include "buffer/buffer_pool_manager_instance.h"
#include "common/logger.h"
#include "container/hash/extendible_hash_table.h"
#include "gtest/gtest.h"
#include "murmur3/MurmurHash3.h"

// Macro for time out mechanism
#define TEST_TIMEOUT_BEGIN                           \
  std::promise<bool> promisedFinished;               \
  auto futureResult = promisedFinished.get_future(); \
                              std::thread([](std::promise<bool>& finished) {
#define TEST_TIMEOUT_FAIL_END(X)                                                                  \
  finished.set_value(true);                                                                       \
  }, std::ref(promisedFinished)).detach();                                                        \
  EXPECT_TRUE(futureResult.wait_for(std::chrono::milliseconds(X)) != std::future_status::timeout) \
      << "Test Failed Due to Time Out";

namespace bustub {

// NOLINTNEXTLINE

// NOLINTNEXTLINE
TEST(HashTableTest, SampleTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(50, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());

  // insert a few values
  for (int i = 0; i < 5; i++) {
    ht.Insert(nullptr, i, i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  // check if the inserted values are all there
  for (int i = 0; i < 5; i++) {
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to keep " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  // insert one more value for each key
  for (int i = 0; i < 5; i++) {
    if (i == 0) {
      // duplicate values for the same key are not allowed
      EXPECT_FALSE(ht.Insert(nullptr, i, 2 * i));
    } else {
      EXPECT_TRUE(ht.Insert(nullptr, i, 2 * i));
    }
    ht.Insert(nullptr, i, 2 * i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    if (i == 0) {
      // duplicate values for the same key are not allowed
      EXPECT_EQ(1, res.size());
      EXPECT_EQ(i, res[0]);
    } else {
      EXPECT_EQ(2, res.size());
      if (res[0] == i) {
        EXPECT_EQ(2 * i, res[1]);
      } else {
        EXPECT_EQ(2 * i, res[0]);
        EXPECT_EQ(i, res[1]);
      }
    }
  }

  ht.VerifyIntegrity();

  // look for a key that does not exist
  std::vector<int> res;
  ht.GetValue(nullptr, 20, &res);
  EXPECT_EQ(0, res.size());

  // delete some values
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(ht.Remove(nullptr, i, i));
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    if (i == 0) {
      // (0, 0) is the only pair with key 0
      EXPECT_EQ(0, res.size());
    } else {
      EXPECT_EQ(1, res.size());
      EXPECT_EQ(2 * i, res[0]);
    }
  }

  ht.VerifyIntegrity();

  // delete all values
  for (int i = 0; i < 5; i++) {
    if (i == 0) {
      // (0, 0) has been deleted
      EXPECT_FALSE(ht.Remove(nullptr, i, 2 * i));
    } else {
      EXPECT_TRUE(ht.Remove(nullptr, i, 2 * i));
    }
  }

  ht.VerifyIntegrity();

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

TEST(HashTableTest, OwnTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(50, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());

  for (int i = 0; i < 500; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, i, i));
  }

  for (int i = 0; i < 500; i++) {
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

#define EACH_BUCKET_SIZE 496

TEST(HashTableTest, GrowShrinkTest1) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(4, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());

  for (int i = 0; i < 500; i++) {
    auto key = i;
    auto value = i;
    EXPECT_TRUE(ht.Insert(nullptr, key, value));
    std::vector<int> res;
    EXPECT_TRUE(ht.GetValue(nullptr, key, &res));
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(value, res[0]);
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < 500; i++) {
    auto key = i;
    auto value = i;
    std::vector<int> res;
    EXPECT_TRUE(ht.GetValue(nullptr, key, &res));
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(value, res[0]);
  }

  ht.VerifyIntegrity();

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

TEST(HashTableTest, GrowShrinkTest2) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(20, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());

  const int data_size = 1000;

  for (int i = 0; i < data_size; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, i, i));
  }

  for (int i = 0; i < data_size; i += 2) {
    ASSERT_TRUE(ht.Remove(nullptr, i, i));
  }

  ht.VerifyIntegrity();

  for (int i = 1; i < data_size; i += 2) {
    std::vector<int> res;
    ASSERT_TRUE(ht.GetValue(nullptr, i, &res));
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(i, res[0]);
    ASSERT_TRUE(ht.Remove(nullptr, i, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < data_size; i++) {
    std::vector<int> res;
    ASSERT_FALSE(ht.GetValue(nullptr, i, &res));
  }

  for (int i = 0; i < data_size; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, i, i));
  }

  for (int i = 0; i < data_size; i += 2) {
    ASSERT_TRUE(ht.Remove(nullptr, i, i));
  }

  ht.VerifyIntegrity();

  for (int i = 1; i < data_size; i += 2) {
    std::vector<int> res;
    ASSERT_TRUE(ht.GetValue(nullptr, i, &res));
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(i, res[0]);
    ASSERT_TRUE(ht.Remove(nullptr, i, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < data_size; i++) {
    std::vector<int> res;
    ASSERT_FALSE(ht.GetValue(nullptr, i, &res));
  }

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

TEST(HashTableTest, LargeInsertTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(30, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());
  for (int i = 0; i < 5000; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, i, i));
  }

  for (int i = 0; i < 5000; i++) {
    std::vector<int> res;
    ASSERT_TRUE(ht.GetValue(nullptr, i, &res));
    ASSERT_EQ(i, res[0]);
  }
  ht.VerifyIntegrity();

  for (int i = 0; i < 2500; i++) {
    ASSERT_TRUE(ht.Remove(nullptr, i, i));
  }

  ht.VerifyIntegrity();

  for (int i = 2500; i < 5000; i++) {
    std::vector<int> res;
    ASSERT_TRUE(ht.GetValue(nullptr, i, &res));
    ASSERT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  for (int i = 2500; i < 5000; i++) {
    ASSERT_TRUE(ht.Remove(nullptr, i, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < 5000; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, i, i));
  }
  ht.VerifyIntegrity();

  for (int i = 0; i < 5000; i++) {
    std::vector<int> res;
    ASSERT_TRUE(ht.GetValue(nullptr, i, &res));
    ASSERT_EQ(i, res[0]);
  }
  ht.VerifyIntegrity();

  for (int i = 0; i < 5000; i++) {
    ASSERT_TRUE(ht.Remove(nullptr, i, i));
  }
  std::vector<int> res;
  ASSERT_FALSE(ht.GetValue(nullptr, 2500, &res));

  ht.VerifyIntegrity();

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

TEST(HashTableTest, SplitInsertTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(30, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());
  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, -1, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, 9, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, 23, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, 11, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, 15, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, 3, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, 338, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, 5, i));
  }

  ht.VerifyIntegrity();

  ASSERT_EQ(4, ht.GetGlobalDepth());

  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Remove(nullptr, -1, i));
    ASSERT_TRUE(ht.Remove(nullptr, 9, i));
    ASSERT_TRUE(ht.Remove(nullptr, 23, i));
    ASSERT_TRUE(ht.Remove(nullptr, 11, i));
    ASSERT_TRUE(ht.Remove(nullptr, 15, i));
    ASSERT_TRUE(ht.Remove(nullptr, 3, i));
    ASSERT_TRUE(ht.Remove(nullptr, 338, i));
    ASSERT_TRUE(ht.Remove(nullptr, 5, i));
  }

  ht.VerifyIntegrity();

  ASSERT_EQ(0, ht.GetGlobalDepth());

  // second times
  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, -1, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, 9, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, 23, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, 11, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, 15, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, 3, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, 338, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, 5, i));
  }

  ht.VerifyIntegrity();

  // todo
  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, 2, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, 351, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, 333, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, 211, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, 6, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, 13, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, 18, i));
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < EACH_BUCKET_SIZE; i++) {
    ASSERT_TRUE(ht.Insert(nullptr, 1, i));
  }

  ht.VerifyIntegrity();

  ASSERT_EQ(4, ht.GetGlobalDepth());

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

// steal

template <typename KeyType>
class ZeroHashFunction : public HashFunction<KeyType> {
  uint64_t GetHash(KeyType key /* unused */) override { return 0; }
};

template <typename KeyType>
KeyType GetKey(int i) {
  KeyType key;
  key.SetFromInteger(i);
  return key;
}

template <>
int GetKey<int>(int i) {
  return i;
}

template <typename ValueType>
ValueType GetValue(int i) {
  return static_cast<ValueType>(i);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void InsertTestCall(KeyType k /* unused */, ValueType v /* unused */, KeyComparator comparator) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(3, disk_manager);
  ExtendibleHashTable<KeyType, ValueType, KeyComparator> ht("blah", bpm, comparator, HashFunction<KeyType>());

  for (int i = 0; i < 10; i++) {
    auto key = GetKey<KeyType>(i);
    auto value = GetValue<ValueType>(i);
    EXPECT_TRUE(ht.Insert(nullptr, key, value));
    std::vector<ValueType> res;
    EXPECT_TRUE(ht.GetValue(nullptr, key, &res));
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(value, res[0]);
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < 10; i++) {
    auto key = GetKey<KeyType>(i);
    auto value = GetValue<ValueType>(i);
    std::vector<ValueType> res;
    EXPECT_TRUE(ht.GetValue(nullptr, key, &res));
    EXPECT_EQ(1, res.size()) << "Failed to keep " << i << std::endl;
    EXPECT_EQ(value, res[0]);
  }

  ht.VerifyIntegrity();

  for (int i = 1; i < 10; i++) {
    auto key = GetKey<KeyType>(i);
    auto value1 = GetValue<ValueType>(i);
    auto value2 = GetValue<ValueType>(2 * i);
    EXPECT_TRUE(ht.Insert(nullptr, key, value2));
    std::vector<ValueType> res;
    EXPECT_TRUE(ht.GetValue(nullptr, key, &res));
    EXPECT_EQ(2, res.size()) << "Failed to insert/get multiple values " << i << std::endl;
    if (res[0] == value1) {
      EXPECT_EQ(value2, res[1]);
    } else {
      EXPECT_EQ(value2, res[0]);
      EXPECT_EQ(value1, res[1]);
    }
  }

  ht.VerifyIntegrity();

  auto key20 = GetKey<KeyType>(20);
  std::vector<ValueType> res;
  EXPECT_FALSE(ht.GetValue(nullptr, key20, &res));
  EXPECT_EQ(0, res.size());

  for (int i = 20; i < 40; i++) {
    auto value = GetValue<ValueType>(i);
    EXPECT_TRUE(ht.Insert(nullptr, key20, value));
    std::vector<ValueType> res;
    EXPECT_TRUE(ht.GetValue(nullptr, key20, &res));
    EXPECT_EQ(i - 19, res.size()) << "Failed to insert " << i << std::endl;
  }

  for (int i = 40; i < 50; i++) {
    auto key = GetKey<KeyType>(i);
    auto value = GetValue<ValueType>(i);
    std::vector<ValueType> res1;
    EXPECT_FALSE(ht.GetValue(nullptr, key, &res1)) << "Found non-existent value: " << i << std::endl;
    EXPECT_TRUE(ht.Insert(nullptr, key, value)) << "Failed to insert value: " << i << std::endl;
    std::vector<ValueType> res2;
    EXPECT_TRUE(ht.GetValue(nullptr, key, &res2)) << "Failed to find value: " << i << std::endl;
    EXPECT_EQ(1, res2.size()) << "Invalid result size for: " << i << std::endl;
    EXPECT_EQ(value, res2[0]);
  }

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void RemoveTestCall(KeyType k /* unused */, ValueType v /* unused */, KeyComparator comparator) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(3, disk_manager);
  ExtendibleHashTable<KeyType, ValueType, KeyComparator> ht("blah", bpm, comparator, HashFunction<KeyType>());

  for (int i = 1; i < 10; i++) {
    auto key = GetKey<KeyType>(i);
    auto value = GetValue<ValueType>(i);
    ht.Insert(nullptr, key, value);
    ht.Remove(nullptr, key, value);
    std::vector<ValueType> res;
    ht.GetValue(nullptr, key, &res);
    EXPECT_EQ(0, res.size());
  }

  ht.VerifyIntegrity();

  for (int i = 1; i < 10; i++) {
    auto key = GetKey<KeyType>(i);
    auto value1 = GetValue<ValueType>(i);
    auto value2 = GetValue<ValueType>(2 * i);
    ht.Insert(nullptr, key, value1);
    ht.Insert(nullptr, key, value2);
    ht.Remove(nullptr, key, value1);
    std::vector<ValueType> res;
    ht.GetValue(nullptr, key, &res);
    EXPECT_EQ(1, res.size());
    EXPECT_EQ(value2, res[0]);
  }

  ht.VerifyIntegrity();

  for (int i = 20; i < 50; i += 2) {
    auto key = GetKey<KeyType>(i);
    auto value = GetValue<ValueType>(i);
    ht.Insert(nullptr, key, value);
    ht.Remove(nullptr, key, value);
    std::vector<ValueType> res;
    ht.GetValue(nullptr, key, &res);
    EXPECT_EQ(0, res.size()) << "Failed to remove " << i << std::endl;
  }

  ht.VerifyIntegrity();

  for (int i = 20; i < 50; i += 2) {
    auto key = GetKey<KeyType>(i);
    auto value1 = GetValue<ValueType>(i);
    auto value2 = GetValue<ValueType>(2 * i);
    ht.Insert(nullptr, key, value1);
    ht.Insert(nullptr, key, value2);
    ht.Remove(nullptr, key, value2);
    ht.Remove(nullptr, key, value1);
    std::vector<ValueType> res;
    ht.GetValue(nullptr, key, &res);
    EXPECT_EQ(0, res.size()) << "Failed to remove " << i << std::endl;
  }

  ht.VerifyIntegrity();

  for (int i = 20; i < 50; i += 2) {
    auto key = GetKey<KeyType>(i);
    auto value2 = GetValue<ValueType>(2 * i);
    ht.Insert(nullptr, key, value2);
  }

  ht.VerifyIntegrity();

  for (int i = 20; i < 50; i += 2) {
    auto key = GetKey<KeyType>(i);
    auto value2 = GetValue<ValueType>(2 * i);
    ht.Remove(nullptr, key, value2);
    std::vector<ValueType> res;
    ht.GetValue(nullptr, key, &res);
    EXPECT_EQ(0, res.size()) << "Failed to remove" << i << std::endl;
  }

  ht.VerifyIntegrity();

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void SplitGrowTestCall(KeyType k /* unused */, ValueType v /* unused */, KeyComparator comparator) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(4, disk_manager);
  ExtendibleHashTable<KeyType, ValueType, KeyComparator> ht("blah", bpm, comparator, HashFunction<KeyType>());

  for (int i = 0; i < 500; i++) {
    auto key = GetKey<KeyType>(i);
    auto value = GetValue<ValueType>(i);
    EXPECT_TRUE(ht.Insert(nullptr, key, value));
    std::vector<ValueType> res;
    EXPECT_TRUE(ht.GetValue(nullptr, key, &res));
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(value, res[0]);
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < 500; i++) {
    auto key = GetKey<KeyType>(i);
    auto value = GetValue<ValueType>(i);
    std::vector<ValueType> res;
    EXPECT_TRUE(ht.GetValue(nullptr, key, &res));
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(value, res[0]);
  }

  ht.VerifyIntegrity();

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void GrowShrinkTestCall(KeyType k /* unused */, ValueType v /* unused */, KeyComparator comparator) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(15, disk_manager);
  ExtendibleHashTable<KeyType, ValueType, KeyComparator> ht("blah", bpm, comparator, HashFunction<KeyType>());

  for (int i = 0; i < 1000; i++) {
    auto key = GetKey<KeyType>(i);
    auto value = GetValue<ValueType>(i);
    EXPECT_TRUE(ht.Insert(nullptr, key, value));
    std::vector<ValueType> res;
    EXPECT_TRUE(ht.GetValue(nullptr, key, &res));
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(value, res[0]);
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < 500; i++) {
    auto key = GetKey<KeyType>(i);
    auto value = GetValue<ValueType>(i);
    ht.Remove(nullptr, key, value);
    std::vector<ValueType> res;
    EXPECT_FALSE(ht.GetValue(nullptr, key, &res));
    EXPECT_EQ(0, res.size()) << "Found non-existent key " << i << std::endl;
  }

  ht.VerifyIntegrity();

  for (int i = 1000; i < 1500; i++) {
    auto key = GetKey<KeyType>(i);
    auto value = GetValue<ValueType>(i);
    EXPECT_TRUE(ht.Insert(nullptr, key, value));
    std::vector<ValueType> res;
    EXPECT_TRUE(ht.GetValue(nullptr, key, &res));
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(value, res[0]);
  }

  ht.VerifyIntegrity();

  for (int i = 500; i < 1000; i++) {
    auto key = GetKey<KeyType>(i);
    auto value = GetValue<ValueType>(i);
    ht.Remove(nullptr, key, value);
    std::vector<ValueType> res;
    EXPECT_FALSE(ht.GetValue(nullptr, key, &res));
    EXPECT_EQ(0, res.size()) << "Found non-existent key " << i << std::endl;
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < 500; i++) {
    auto key = GetKey<KeyType>(i);
    auto value = GetValue<ValueType>(i);
    EXPECT_TRUE(ht.Insert(nullptr, key, value));
    std::vector<ValueType> res;
    EXPECT_TRUE(ht.GetValue(nullptr, key, &res));
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(value, res[0]);
  }

  ht.VerifyIntegrity();

  for (int i = 1000; i < 1500; i++) {
    auto key = GetKey<KeyType>(i);
    auto value = GetValue<ValueType>(i);
    ht.Remove(nullptr, key, value);
    std::vector<ValueType> res;
    EXPECT_FALSE(ht.GetValue(nullptr, key, &res));
    EXPECT_EQ(0, res.size()) << "Found non-existent key " << i << std::endl;
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < 500; i++) {
    auto key = GetKey<KeyType>(i);
    auto value = GetValue<ValueType>(i);
    ht.Remove(nullptr, key, value);
    std::vector<ValueType> res;
    EXPECT_FALSE(ht.GetValue(nullptr, key, &res));
    EXPECT_EQ(0, res.size()) << "Found non-existent key " << i << std::endl;
  }

  ht.VerifyIntegrity();

  //  remove everything and make sure global depth < max_global_depth
  for (int i = 0; i < 1500; i++) {
    auto key = GetKey<KeyType>(i);
    auto value = GetValue<ValueType>(i);
    ht.Remove(nullptr, key, value);
  }

  assert(ht.GetGlobalDepth() <= 1);
  ht.VerifyIntegrity();

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void GenericTestCall(void (*func)(KeyType, ValueType, KeyComparator)) {
  Schema schema(std::vector<Column>({Column("A", TypeId::BIGINT)}));
  KeyComparator comparator(&schema);
  auto key = GetKey<KeyType>(0);
  auto value = GetValue<ValueType>(0);
  func(key, value, comparator);
}

TEST(HashTableTest, InsertTest) {
  InsertTestCall(1, 1, IntComparator());

  GenericTestCall<GenericKey<8>, RID, GenericComparator<8>>(InsertTestCall);
  GenericTestCall<GenericKey<16>, RID, GenericComparator<16>>(InsertTestCall);
  GenericTestCall<GenericKey<32>, RID, GenericComparator<32>>(InsertTestCall);
  GenericTestCall<GenericKey<64>, RID, GenericComparator<64>>(InsertTestCall);
}

TEST(HashTableTest, RemoveTest) {
  RemoveTestCall(1, 1, IntComparator());

  GenericTestCall<GenericKey<8>, RID, GenericComparator<8>>(RemoveTestCall);
  GenericTestCall<GenericKey<16>, RID, GenericComparator<16>>(RemoveTestCall);
  GenericTestCall<GenericKey<32>, RID, GenericComparator<32>>(RemoveTestCall);
  GenericTestCall<GenericKey<64>, RID, GenericComparator<64>>(RemoveTestCall);
}

TEST(HashTableTest, SplitGrowTest) {
  SplitGrowTestCall(1, 1, IntComparator());

  GenericTestCall<GenericKey<8>, RID, GenericComparator<8>>(SplitGrowTestCall);
  GenericTestCall<GenericKey<16>, RID, GenericComparator<16>>(SplitGrowTestCall);
  GenericTestCall<GenericKey<32>, RID, GenericComparator<32>>(SplitGrowTestCall);
  GenericTestCall<GenericKey<64>, RID, GenericComparator<64>>(SplitGrowTestCall);
}

TEST(HashTableTest, GrowShrinkTest) {
  GrowShrinkTestCall(1, 1, IntComparator());

  GenericTestCall<GenericKey<8>, RID, GenericComparator<8>>(GrowShrinkTestCall);
  GenericTestCall<GenericKey<16>, RID, GenericComparator<16>>(GrowShrinkTestCall);
  GenericTestCall<GenericKey<32>, RID, GenericComparator<32>>(GrowShrinkTestCall);
  GenericTestCall<GenericKey<64>, RID, GenericComparator<64>>(GrowShrinkTestCall);
}

TEST(HashTableTest, IntegratedConcurrencyTest) {
  const int num_threads = 5;
  const int num_runs = 50;

  for (int run = 0; run < num_runs; run++) {
    auto *disk_manager = new DiskManager("test.db");
    auto *bpm = new BufferPoolManagerInstance(50, disk_manager);

    ExtendibleHashTable<int, int, IntComparator> *ht =
        new ExtendibleHashTable<int, int, IntComparator>("blah", bpm, IntComparator(), HashFunction<int>());
    std::vector<std::thread> threads(num_threads);

    for (int tid = 0; tid < num_threads; tid++) {
      threads[tid] = std::thread([&ht, tid]() {
        ht->Insert(nullptr, tid, tid);
        std::vector<int> res;
        ht->GetValue(nullptr, tid, &res);
        EXPECT_EQ(1, res.size()) << "Failed to insert " << tid << std::endl;
        EXPECT_EQ(tid, res[0]);
      });
    }

    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    threads.clear();

    threads.resize(num_threads);
    for (int tid = 0; tid < num_threads; tid++) {
      threads[tid] = std::thread([&ht, tid]() {
        ht->Remove(nullptr, tid, tid);
        std::vector<int> res;
        ht->GetValue(nullptr, tid, &res);
        EXPECT_EQ(0, res.size());
      });
    }

    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    threads.clear();

    threads.resize(num_threads);
    for (int tid = 0; tid < num_threads; tid++) {
      threads[tid] = std::thread([&ht, tid]() {
        // LOG_DEBUG("thread %d\n",tid);
        ht->Insert(nullptr, 1, tid);
        std::vector<int> res;
        ht->GetValue(nullptr, 1, &res);
        bool found = false;
        for (auto r : res) {
          if (r == tid) {
            found = true;
          }
        }
        EXPECT_EQ(true, found);
      });
    }

    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    std::vector<int> res;
    ht->GetValue(nullptr, 1, &res);

    EXPECT_EQ(num_threads, res.size());

    delete ht;
    disk_manager->ShutDown();
    remove("test.db");
    delete disk_manager;
    delete bpm;
  }
}

TEST(HashTableTest, GrowShrinkConcurrencyTest) {
  const int num_threads = 5;
  const int num_runs = 50;

  for (int run = 0; run < num_runs; run++) {
    auto *disk_manager = new DiskManager("test.db");
    auto *bpm = new BufferPoolManagerInstance(50, disk_manager);
    ExtendibleHashTable<int, int, IntComparator> *ht =
        new ExtendibleHashTable<int, int, IntComparator>("blah", bpm, IntComparator(), HashFunction<int>());
    std::vector<std::thread> threads(num_threads);

    for (int tid = 0; tid < num_threads; tid++) {
      threads[tid] = std::thread([&ht, tid]() {
        for (int i = num_threads * tid; i < num_threads * (tid + 1); i++) {
          ht->Insert(nullptr, i, i);
          std::vector<int> res;
          ht->GetValue(nullptr, i, &res);
          EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
          EXPECT_EQ(i, res[0]);
        }
      });
    }

    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    threads.clear();

    threads.resize(num_threads);
    for (int tid = 0; tid < num_threads; tid++) {
      threads[tid] = std::thread([&ht, tid]() {
        for (int i = num_threads * tid; i < num_threads * (tid + 1); i++) {
          std::vector<int> res;
          ht->GetValue(nullptr, i, &res);
          EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
          EXPECT_EQ(i, res[0]);
        }
      });
    }

    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    threads.clear();

    threads.resize(num_threads);
    for (int tid = 0; tid < num_threads; tid++) {
      threads[tid] = std::thread([&ht, tid]() {
        for (int i = num_threads * tid; i < num_threads * (tid + 1); i++) {
          ht->Insert(nullptr, i, i);
          std::vector<int> res;
          ht->GetValue(nullptr, i, &res);
          EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
        }
        for (int i = num_threads * tid; i < num_threads * (tid + 1); i++) {
          assert(ht->Remove(nullptr, i, i));
          std::vector<int> res;
          ht->GetValue(nullptr, i, &res);
          EXPECT_EQ(0, res.size()) << "Failed to insert " << tid << std::endl;
        }
      });
    }

    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    threads.clear();
    delete ht;
    disk_manager->ShutDown();
    remove("test.db");
    delete disk_manager;
    delete bpm;
  }
}

template <typename... Args>
void LaunchParallelTest(uint64_t num_threads, uint64_t txn_id_start, Args &&...args) {
  std::vector<std::thread> thread_group;

  // Launch a group of threads
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.emplace_back(std::thread(args..., txn_id_start + thread_itr, thread_itr));
  }

  // Join the threads with the main thread
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }
}

// helper function to insert
void InsertHelper(ExtendibleHashTable<int, int, IntComparator> *hash_table, const std::vector<int> &keys, uint64_t tid,
                  __attribute__((unused)) uint64_t thread_itr = 0) {
  for (auto key : keys) {
    int value = key;
    //    LOG_DEBUG("INSERT %d--%d", key, value);
    hash_table->Insert(nullptr, key, value);
  }
  EXPECT_NE(keys[0], keys[1]);
}

// helper function to seperate insert
void InsertHelperSplit(ExtendibleHashTable<int, int, IntComparator> *hash_table, const std::vector<int> &keys,
                       int total_threads, uint64_t tid, __attribute__((unused)) uint64_t thread_itr) {
  for (auto key : keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      int value = key;
      hash_table->Insert(nullptr, key, value);
    }
  }
}

// helper function to delete
void DeleteHelper(ExtendibleHashTable<int, int, IntComparator> *hash_table, const std::vector<int> &remove_keys,
                  uint64_t tid, __attribute__((unused)) uint64_t thread_itr = 0) {
  for (auto key : remove_keys) {
    int value = key;
    hash_table->Remove(nullptr, key, value);
  }
}

// helper function to seperate delete
void DeleteHelperSplit(ExtendibleHashTable<int, int, IntComparator> *hash_table, const std::vector<int> &remove_keys,
                       int total_threads, uint64_t tid, __attribute__((unused)) uint64_t thread_itr) {
  for (auto key : remove_keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      int value = key;
      hash_table->Remove(nullptr, key, value);
    }
  }
}

void LookupHelper(ExtendibleHashTable<int, int, IntComparator> *hash_table, const std::vector<int> &keys, uint64_t tid,
                  __attribute__((unused)) uint64_t thread_itr = 0) {
  for (auto key : keys) {
    int value = key;
    std::vector<int> result;
    bool res = hash_table->GetValue(nullptr, key, &result);
    EXPECT_EQ(res, true) << "Fail to Get Key:" << key;
    EXPECT_EQ(result.size(), 1);
    EXPECT_EQ(result[0], value);
  }
}

void ConcurrentScaleTest() {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(13, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> hash_table("foo_pk", bpm, IntComparator(), HashFunction<int>());

  // Create header_page
  page_id_t page_id;
  bpm->NewPage(&page_id, nullptr);

  // Add perserved_keys
  std::vector<int> perserved_keys;
  std::vector<int> dynamic_keys;
  size_t total_keys = 50000;
  size_t sieve = 10;
  for (size_t i = 1; i <= total_keys; i++) {
    if (i % sieve == 0) {
      perserved_keys.emplace_back(i);
    } else {
      dynamic_keys.emplace_back(i);
    }
  }
  InsertHelper(&hash_table, perserved_keys, 1);

  size_t size;

  auto insert_task = [&](int tid) { InsertHelper(&hash_table, dynamic_keys, tid); };
  // auto insert_task = [&](int tid) {  };
  auto delete_task = [&](int tid) { DeleteHelper(&hash_table, dynamic_keys, tid); };
  // auto delete_task = [&](int tid) { };
  auto lookup_task = [&](int tid) { LookupHelper(&hash_table, perserved_keys, tid); };
  // auto lookup_task = [&](int tid) { };

  std::vector<std::thread> threads;
  std::vector<std::function<void(int)>> tasks;
  tasks.emplace_back(insert_task);
  tasks.emplace_back(delete_task);
  tasks.emplace_back(lookup_task);

  size_t num_threads = 6;
  for (size_t i = 0; i < num_threads; i++) {
    threads.emplace_back(std::thread{tasks[i % tasks.size()], i});
  }
  for (size_t i = 0; i < num_threads; i++) {
    threads[i].join();
  }

  std::cout << hash_table.Size() << std::endl;
  //  LOG_DEBUG("Insert success");

  // Check all reserved keys exist
  size = 0;
  std::vector<int> result;
  for (auto key : perserved_keys) {
    result.clear();
    int value = key;
    hash_table.GetValue(nullptr, key, &result);
    if (std::find(result.begin(), result.end(), value) != result.end()) {
      size++;
    }
  }
  //  LOG_DEBUG("Check success");
  EXPECT_EQ(size, perserved_keys.size());

  hash_table.VerifyIntegrity();

  // Cleanup
  bpm->UnpinPage(HEADER_PAGE_ID, true);
  disk_manager->ShutDown();
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

void ScaleTestCall() {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(4, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("foo_pk", bpm, IntComparator(), HashFunction<int>());

  int num_keys = 100000;  // index can fit around 225k int-int pairs

  // Create header_page
  page_id_t page_id;
  bpm->NewPage(&page_id, nullptr);

  //  insert all the keys
  for (int i = 0; i < num_keys; i++) {
    ht.Insert(nullptr, i, i);
    std::vector<int> res;
    EXPECT_TRUE(ht.GetValue(nullptr, i, &res));
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
  }

  ht.VerifyIntegrity();

  //  remove half the keys
  for (int i = 0; i < num_keys / 2; i++) {
    EXPECT_TRUE(ht.Remove(nullptr, i, i));
    std::vector<int> res;
    EXPECT_FALSE(ht.GetValue(nullptr, i, &res));
    EXPECT_EQ(0, res.size()) << "Found non-existent key " << i << std::endl;
  }

  ht.VerifyIntegrity();

  //  try to find the removed half
  for (int i = 0; i < num_keys / 2; i++) {
    std::vector<int> res;
    EXPECT_FALSE(ht.GetValue(nullptr, i, &res));
  }

  //  insert to the 2nd half as duplicates
  for (int i = num_keys / 2; i < num_keys; i++) {
    ht.Insert(nullptr, i, i + 1);
    std::vector<int> res;
    EXPECT_TRUE(ht.GetValue(nullptr, i, &res));
    EXPECT_EQ(2, res.size()) << "Missing duplicate kv pair for: " << i << std::endl;
  }

  ht.VerifyIntegrity();

  //  get all the duplicates
  for (int i = num_keys / 2; i < num_keys; i++) {
    std::vector<int> res;
    EXPECT_TRUE(ht.GetValue(nullptr, i, &res));
    EXPECT_EQ(2, res.size()) << "Missing duplicate kv pair for: " << i << std::endl;
  }

  ht.VerifyIntegrity();

  //  remove the last duplicates inserted
  for (int i = num_keys / 2; i < num_keys; i++) {
    EXPECT_TRUE(ht.Remove(nullptr, i, i + 1));
    std::vector<int> res;
    EXPECT_TRUE(ht.GetValue(nullptr, i, &res));
    EXPECT_EQ(1, res.size()) << "Missing kv pair for: " << i << std::endl;
  }

  ht.VerifyIntegrity();

  //  query everything
  for (int i = num_keys / 2; i < num_keys; i++) {
    std::vector<int> res;
    EXPECT_TRUE(ht.GetValue(nullptr, i, &res));
    EXPECT_EQ(1, res.size()) << "Missing kv pair for: " << i << std::endl;
  }

  ht.VerifyIntegrity();

  //  remove the rest of the remaining keys
  for (int i = num_keys / 2; i < num_keys; i++) {
    EXPECT_TRUE(ht.Remove(nullptr, i, i));
    std::vector<int> res;
    EXPECT_FALSE(ht.GetValue(nullptr, i, &res));
    EXPECT_EQ(0, res.size()) << "Failed to insert " << i << std::endl;
  }

  ht.VerifyIntegrity();

  //  query everything
  for (int i = 0; i < num_keys; i++) {
    std::vector<int> res;
    EXPECT_FALSE(ht.GetValue(nullptr, i, &res));
    EXPECT_EQ(0, res.size()) << "Found non-existent key: " << i << std::endl;
  }

  //  Verify Merging Worked
  assert(ht.GetGlobalDepth() < 8);
  ht.VerifyIntegrity();

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

/*
 * Score: 5
 * Description: Insert 200k keys to verify the table capacity
 */
TEST(HashTableTest, ScaleTest) { ScaleTestCall(); }

/*
 * Score: 5
 * Description: Same as MixTest2 but with 100k integer keys
 * and only runs 1 iteration.
 */
TEST(HashTableTest, ConcurrentScaleTest) {
  TEST_TIMEOUT_BEGIN
  ConcurrentScaleTest();
  TEST_TIMEOUT_FAIL_END(3 * 1000 * 120)
}

}  // namespace bustub
