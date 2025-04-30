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
#include <zlib.h>
#include <Core/Settings.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Parser/SerializedPlanParser.h>
#include <Storages/SubstraitSource/Delta/Bitmap/DeltaDVRoaringBitmapArray.h>
#include <Storages/SubstraitSource/ReadBufferBuilder.h>
#include <gtest/gtest.h>
#include <tests/utils/gluten_test_util.h>
#include <roaring.hh>
#include <Common/Base85Codec.h>
#include <Common/QueryContext.h>

namespace DB::Setting
{
extern const SettingsBool enable_named_columns_in_function_tuple;
}
using namespace local_engine;

using namespace DB;

TEST(Delta_DV, roaring_bitmap_test)
{
    const auto context = DB::Context::createCopy(QueryContext::globalContext());

    const Poco::URI file_uri(test::gtest_data("deletion_vector_only_one.bin"));

    ReadBufferFromFile in(file_uri.getPath());

    char a;
    readBinary(a, in);
    EXPECT_EQ('\x01', a);

    int size;
    readBinaryBigEndian(size, in);
    EXPECT_EQ(539, size);

    int magic_num;
    readBinaryLittleEndian(magic_num, in);
    EXPECT_EQ(1681511377, magic_num);

    int64_t bitmap_array_size;
    readBinaryLittleEndian(bitmap_array_size, in);
    EXPECT_EQ(1, bitmap_array_size);

    std::vector<roaring::Roaring> roaring_bitmap_array;
    roaring_bitmap_array.reserve(bitmap_array_size);

    for (size_t i = 0; i < bitmap_array_size; ++i)
    {
        int bitmap_index;
        readBinaryLittleEndian(bitmap_index, in);
        roaring::Roaring r = roaring::Roaring::read(in.position());
        size_t current_bitmap_size = r.getSizeInBytes();
        in.ignore(current_bitmap_size);
        roaring_bitmap_array.push_back(r);

        int check_sum;
        readBinaryBigEndian(check_sum, in);
        EXPECT_EQ(1722047305, check_sum);

        EXPECT_TRUE(r.contains(0));
        EXPECT_TRUE(r.contains(1003));
        EXPECT_TRUE(r.contains(666));
        EXPECT_FALSE(r.contains(6));
        EXPECT_FALSE(r.contains(26));
        EXPECT_FALSE(r.contains(188));
    }
}

TEST(Delta_DV, multi_roaring_bitmap_test)
{
    const auto context = DB::Context::createCopy(QueryContext::globalContext());

    const Poco::URI file_uri(test::gtest_data("deletion_vector_multiple.bin"));

    ReadBufferFromFile in(file_uri.getPath());

    in.seek(426433, SEEK_SET);

    int size;
    readBinaryBigEndian(size, in);
    EXPECT_EQ(426424, size);

    int checksum_value = static_cast<Int32>(crc32_z(0L, reinterpret_cast<unsigned char*>(in.position()), size));

    int magic_num;
    readBinaryLittleEndian(magic_num, in);
    EXPECT_EQ(1681511377, magic_num);

    int64_t bitmap_array_size;
    readBinaryLittleEndian(bitmap_array_size, in);
    EXPECT_EQ(1, bitmap_array_size);

    std::vector<roaring::Roaring> roaring_bitmap_array;
    roaring_bitmap_array.reserve(bitmap_array_size);

    for (size_t i = 0; i < bitmap_array_size; ++i)
    {
        int bitmap_index;
        readBinaryLittleEndian(bitmap_index, in);
        roaring::Roaring r = roaring::Roaring::read(in.position());
        size_t current_bitmap_size = r.getSizeInBytes();
        in.ignore(current_bitmap_size);
        roaring_bitmap_array.push_back(r);

        EXPECT_TRUE(r.contains(5));
        EXPECT_TRUE(r.contains(3618));
        EXPECT_TRUE(r.contains(155688));
        EXPECT_FALSE(r.contains(3));
        EXPECT_FALSE(r.contains(6886));
        EXPECT_FALSE(r.contains(129900));
    }

    int expected_checksum;
    readBinaryBigEndian(expected_checksum, in);
    EXPECT_EQ(expected_checksum, checksum_value);
}

TEST(Delta_DV, DeltaDVRoaringBitmapArray)
{
    const auto context = DB::Context::createCopy(QueryContext::globalContext());

    const std::string file_uri(test::gtest_uri("deletion_vector_multiple.bin"));
    const std::string file_uri1(test::gtest_uri("deletion_vector_only_one.bin"));

    DeltaDVRoaringBitmapArray bitmap_array{};
    bitmap_array.rb_read(file_uri, 426433, 426424, context);
    EXPECT_TRUE(bitmap_array.rb_contains(5));
    EXPECT_TRUE(bitmap_array.rb_contains(3618));
    EXPECT_TRUE(bitmap_array.rb_contains(155688));
    EXPECT_FALSE(bitmap_array.rb_contains(3));
    EXPECT_FALSE(bitmap_array.rb_contains(6886));
    EXPECT_FALSE(bitmap_array.rb_contains(129900));
    EXPECT_FALSE(bitmap_array.rb_contains(0));
    EXPECT_FALSE(bitmap_array.rb_contains(1003));
    EXPECT_FALSE(bitmap_array.rb_contains(880));

    bitmap_array.rb_add(3000000000);
    EXPECT_TRUE(bitmap_array.rb_contains(3000000000));
    bitmap_array.rb_add(5000000000);
    EXPECT_TRUE(bitmap_array.rb_contains(5000000000));
    bitmap_array.rb_add(10000000000);
    EXPECT_TRUE(bitmap_array.rb_contains(10000000000));

    DeltaDVRoaringBitmapArray bitmap_array1{};
    bitmap_array1.rb_read(file_uri1, 1, 539, context);
    EXPECT_TRUE(bitmap_array1.rb_contains(0));
    EXPECT_TRUE(bitmap_array1.rb_contains(1003));
    EXPECT_TRUE(bitmap_array1.rb_contains(880));
    EXPECT_FALSE(bitmap_array1.rb_contains(6));
    EXPECT_FALSE(bitmap_array1.rb_contains(26));
    EXPECT_FALSE(bitmap_array1.rb_contains(188));

    bitmap_array.rb_merge(bitmap_array1);
    EXPECT_TRUE(bitmap_array.rb_contains(0));
    EXPECT_TRUE(bitmap_array.rb_contains(1003));
    EXPECT_TRUE(bitmap_array.rb_contains(880));

    const std::string file_uri2(test::gtest_uri("deletion_vector_long_values.bin"));

    DeltaDVRoaringBitmapArray bitmap_array2{};
    bitmap_array2.rb_read(file_uri2, 1, 4047, context);
    EXPECT_FALSE(bitmap_array2.rb_is_empty());
    EXPECT_EQ(2098, bitmap_array2.cardinality());
    EXPECT_TRUE(bitmap_array2.rb_contains(0));
    EXPECT_TRUE(bitmap_array2.rb_contains(1003));
    EXPECT_TRUE(bitmap_array2.rb_contains(880));
    Int64 v = 2000000010;
    EXPECT_TRUE(bitmap_array2.rb_contains(v));
    UInt64 vv = 2000000010;
    EXPECT_TRUE(bitmap_array2.rb_contains(vv));
    v = 3000000990;
    EXPECT_TRUE(bitmap_array2.rb_contains(v));
    vv = 3000000990;
    EXPECT_TRUE(bitmap_array2.rb_contains(vv));
    v = 3000000990;
    EXPECT_TRUE(bitmap_array2.rb_contains(v));
    vv = 3000000990;
    EXPECT_TRUE(bitmap_array2.rb_contains(vv));
    v = 5000000298;
    EXPECT_TRUE(bitmap_array2.rb_contains(v));
    vv = 5000000298;
    EXPECT_TRUE(bitmap_array2.rb_contains(vv));
    v = 10000000099;
    EXPECT_TRUE(bitmap_array2.rb_contains(v));
    vv = 10000000099;
    EXPECT_TRUE(bitmap_array2.rb_contains(vv));
    v = 100000000050;
    EXPECT_TRUE(bitmap_array2.rb_contains(v));
    vv = 100000000050;
    EXPECT_TRUE(bitmap_array2.rb_contains(vv));

    EXPECT_FALSE(bitmap_array2.rb_contains(6));
    EXPECT_FALSE(bitmap_array2.rb_contains(26));
    EXPECT_FALSE(bitmap_array2.rb_contains(188));

    bitmap_array2.rb_clear();
    EXPECT_TRUE(bitmap_array2.rb_is_empty());

    DeltaDVRoaringBitmapArray bitmap_array3{};
    bitmap_array3.rb_add(3000000000);
    bitmap_array3.rb_add(5000000000);
    bitmap_array3.rb_add(10000000000);
    EXPECT_TRUE(bitmap_array3.rb_contains(3000000000));
    EXPECT_TRUE(bitmap_array3.rb_contains(5000000000));
    EXPECT_TRUE(bitmap_array3.rb_contains(10000000000));
    EXPECT_FALSE(bitmap_array3.rb_contains(10000000001));
    EXPECT_FALSE(bitmap_array3.rb_contains(5000000001));
    EXPECT_FALSE(bitmap_array3.rb_contains(3000000001));
}

TEST(Delta_DV, Base85Codec)
{
    const String uuid_str = "a5d455b6-92f1-4c89-a26d-93a1d5ce3e89";
    DB::ReadBufferFromString rb = DB::ReadBufferFromString(uuid_str);
    UUID uuid;
    readUUIDText(uuid, rb);

    const String encoded = Base85Codec::encodeUUID(uuid);
    EXPECT_EQ("RpnINLjqk5Qhu9/!Y{vn", encoded);
    auto decodeUUID = Base85Codec::decodeUUID(encoded);
    EXPECT_EQ(uuid_str, toString(decodeUUID));
}