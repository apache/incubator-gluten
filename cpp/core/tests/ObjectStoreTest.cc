/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "utils/ObjectStore.h"
#include <gtest/gtest.h>

using namespace gluten;

TEST(ObjectStore, retreive) {
  auto store = ObjectStore::create();
  auto obj = std::make_shared<int32_t>(1);
  auto handle = store->save(obj);
  auto retrieved = ObjectStore::retrieve<int32_t>(handle);
  ASSERT_EQ(*retrieved, 1);
}

TEST(ObjectStore, retreiveMultiple) {
  auto store = ObjectStore::create();
  auto obj1 = std::make_shared<int32_t>(50);
  auto obj2 = std::make_shared<int32_t>(100);
  auto handle1 = store->save(obj1);
  auto handle2 = store->save(obj2);
  auto retrieved1 = ObjectStore::retrieve<int32_t>(handle1);
  auto retrieved2 = ObjectStore::retrieve<int32_t>(handle2);
  ASSERT_EQ(*retrieved1, *obj1);
  ASSERT_EQ(*retrieved2, *obj2);
}

TEST(ObjectStore, release) {
  ObjectHandle handle = kInvalidObjectHandle;
  auto store = ObjectStore::create();
  {
    auto obj = std::make_shared<int32_t>(1);
    handle = store->save(obj);
  }
  auto retrieved = ObjectStore::retrieve<int32_t>(handle);
  ASSERT_EQ(*retrieved, 1);
  ObjectStore::release(handle);
  ASSERT_ANY_THROW(ObjectStore::retrieve<int32_t>(handle));
}

TEST(ObjectStore, releaseMultiple) {
  ObjectHandle handle1 = kInvalidObjectHandle;
  ObjectHandle handle2 = kInvalidObjectHandle;
  auto store = ObjectStore::create();
  {
    auto obj1 = std::make_shared<int32_t>(50);
    auto obj2 = std::make_shared<int32_t>(100);
    handle1 = store->save(obj1);
    handle2 = store->save(obj2);
  }
  ASSERT_EQ(*ObjectStore::retrieve<int32_t>(handle1), 50);
  ASSERT_EQ(*ObjectStore::retrieve<int32_t>(handle2), 100);
  ObjectStore::release(handle2);
  ASSERT_EQ(*ObjectStore::retrieve<int32_t>(handle1), 50);
  ASSERT_ANY_THROW(ObjectStore::retrieve<int32_t>(handle2));
  ObjectStore::release(handle1);
  ASSERT_ANY_THROW(ObjectStore::retrieve<int32_t>(handle1));
  ASSERT_ANY_THROW(ObjectStore::retrieve<int32_t>(handle2));
}

TEST(ObjectStore, releaseObjectsInMultipleStores) {
  ObjectHandle handle1 = kInvalidObjectHandle;
  ObjectHandle handle2 = kInvalidObjectHandle;
  auto store1 = ObjectStore::create();
  auto store2 = ObjectStore::create();
  {
    auto obj1 = std::make_shared<int32_t>(50);
    auto obj2 = std::make_shared<int32_t>(100);
    handle1 = store1->save(obj1);
    handle2 = store2->save(obj2);
  }
  ASSERT_EQ(*ObjectStore::retrieve<int32_t>(handle1), 50);
  ASSERT_EQ(*ObjectStore::retrieve<int32_t>(handle2), 100);
  ObjectStore::release(handle2);
  ASSERT_EQ(*ObjectStore::retrieve<int32_t>(handle1), 50);
  ASSERT_ANY_THROW(ObjectStore::retrieve<int32_t>(handle2));
  ObjectStore::release(handle1);
  ASSERT_ANY_THROW(ObjectStore::retrieve<int32_t>(handle1));
  ASSERT_ANY_THROW(ObjectStore::retrieve<int32_t>(handle2));
}

TEST(ObjectStore, releaseMultipleStores) {
  ObjectHandle handle1 = kInvalidObjectHandle;
  ObjectHandle handle2 = kInvalidObjectHandle;
  auto store1 = ObjectStore::create();
  auto store2 = ObjectStore::create();
  {
    auto obj1 = std::make_shared<int32_t>(50);
    auto obj2 = std::make_shared<int32_t>(100);
    handle1 = store1->save(obj1);
    handle2 = store2->save(obj2);
  }
  ASSERT_EQ(*ObjectStore::retrieve<int32_t>(handle1), 50);
  ASSERT_EQ(*ObjectStore::retrieve<int32_t>(handle2), 100);
  store2.reset();
  ASSERT_EQ(*ObjectStore::retrieve<int32_t>(handle1), 50);
  ASSERT_ANY_THROW(ObjectStore::retrieve<int32_t>(handle2));
  store1.reset();
  ASSERT_ANY_THROW(ObjectStore::retrieve<int32_t>(handle1));
  ASSERT_ANY_THROW(ObjectStore::retrieve<int32_t>(handle2));
}
