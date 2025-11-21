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

#include <arrow/util/compression.h>

#include "memory/VeloxMemoryManager.h"
#include "shuffle/VeloxRssSortShuffleWriter.h"
#include "tests/VeloxShuffleWriterTestBase.h"
#include "tests/utils/TestUtils.h"

#include "velox/buffer/Buffer.h"
#include "velox/type/Type.h"
#include "velox/vector/tests/VectorTestUtils.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::test;

namespace gluten {

class VeloxRssSortShuffleWriterTest : public VeloxShuffleWriterTestBase, public testing::Test {
 protected:
  static void SetUpTestSuite() {
    setUpVeloxBackend();
  }

  static void TearDownTestSuite() {
    tearDownVeloxBackend();
  }

  void SetUp() override {
    setUpTestData();
  }

  std::shared_ptr<VeloxShuffleWriter> createShuffleWriter(uint32_t numPartitions) {
    auto options = std::make_shared<RssPartitionWriterOptions>();
    auto writerOptions = std::make_shared<RssSortShuffleWriterOptions>();
    auto rssClient = std::make_unique<LocalRssClient>(dataFile_);
    std::unique_ptr<arrow::util::Codec> codec;
    if (writerOptions->compressionType == arrow::Compression::type::UNCOMPRESSED) {
      codec = nullptr;
    } else {
      GLUTEN_ASSIGN_OR_THROW(codec, arrow::util::Codec::Create(writerOptions->compressionType));
    }
    auto partitionWriter = std::make_shared<RssPartitionWriter>(
        numPartitions, std::move(codec), getDefaultMemoryManager(), options, std::move(rssClient));
    GLUTEN_ASSIGN_OR_THROW(
        auto shuffleWriter,
        VeloxRssSortShuffleWriter::create(
            numPartitions, std::move(partitionWriter), std::move(writerOptions), getDefaultMemoryManager()));
    return shuffleWriter;
  }
};

TEST_F(VeloxRssSortShuffleWriterTest, calculateBatchesSize) {
  auto shuffleWriter = std::dynamic_pointer_cast<VeloxRssSortShuffleWriter>(createShuffleWriter(10));
  // Do not trigger resetBatches by shuffle writer.
  const int64_t memLimit = INT64_MAX;

  // Shared string buffer in FlatVector<StringView>.
  BufferPtr strBuffer = AlignedBuffer::allocate<char>(200, pool());
  auto vector1 = BaseVector::create<FlatVector<StringView>>(VARCHAR(), 100, pool());
  vector1->setStringBuffers({strBuffer});
  auto vector2 = BaseVector::create<FlatVector<StringView>>(VARCHAR(), 100, pool());
  vector2->setStringBuffers({strBuffer});
  auto vector3 = BaseVector::create<FlatVector<StringView>>(VARCHAR(), 100, pool());
  vector3->setStringBuffers({strBuffer});
  auto vector4 = BaseVector::create<FlatVector<int64_t>>(INTEGER(), 100, pool());

  auto rowVector1 = makeRowVector({vector1, vector2});
  auto rowVector2 = makeRowVector({vector3, vector4});
  std::shared_ptr<ColumnarBatch> cb1 = std::make_shared<VeloxColumnarBatch>(rowVector1);
  std::shared_ptr<ColumnarBatch> cb2 = std::make_shared<VeloxColumnarBatch>(rowVector2);

  ASSERT_NOT_OK(shuffleWriter->write(cb1, memLimit));
  ASSERT_NOT_OK(shuffleWriter->write(cb2, memLimit));
  auto expectedSize = rowVector1->retainedSize() + rowVector2->retainedSize() - strBuffer->capacity() * 2;
  EXPECT_EQ(expectedSize, shuffleWriter->getInputColumnBytes());
  shuffleWriter->resetBatches();

  // Shared string buffer in ArrayVector.
  BufferPtr offsets = allocateOffsets(1, vector1->pool());
  BufferPtr sizes = allocateOffsets(1, vector1->pool());
  sizes->asMutable<vector_size_t>()[0] = vector1->size();

  auto arrayVector =
      std::make_shared<facebook::velox::ArrayVector>(pool(), ARRAY(VARCHAR()), nullptr, 1, offsets, sizes, vector1);
  auto rowVector3 = makeRowVector({arrayVector, vector1});
  cb1 = std::make_shared<VeloxColumnarBatch>(rowVector3);
  ASSERT_NOT_OK(shuffleWriter->write(cb1, memLimit));
  expectedSize = rowVector3->retainedSize() - strBuffer->capacity();
  EXPECT_EQ(expectedSize, shuffleWriter->getInputColumnBytes());
  shuffleWriter->resetBatches();

  // Shared string buffer in MapVector.
  auto keys = vector1;
  auto values = vector2;
  auto mapVector = makeMapVector({0, 10, 20, 50}, keys, values);
  auto rowVector4 = makeRowVector({mapVector, vector3});
  cb1 = std::make_shared<VeloxColumnarBatch>(rowVector4);
  ASSERT_NOT_OK(shuffleWriter->write(cb1, memLimit));
  expectedSize = rowVector4->retainedSize() - strBuffer->capacity() * 2;
  EXPECT_EQ(expectedSize, shuffleWriter->getInputColumnBytes());
  shuffleWriter->resetBatches();

  // Shared string buffer in RowVector.
  auto rowVector5 = makeRowVector({rowVector1, vector3});
  cb1 = std::make_shared<VeloxColumnarBatch>(rowVector5);
  ASSERT_NOT_OK(shuffleWriter->write(cb1, memLimit));
  expectedSize = rowVector5->retainedSize() - strBuffer->capacity() * 2;
  EXPECT_EQ(expectedSize, shuffleWriter->getInputColumnBytes());
  shuffleWriter->resetBatches();

  // Vector is not flatten.
  auto dictionaryVector = BaseVector::wrapInDictionary(
      BufferPtr(nullptr),
      makeIndices(vector1->size(), [](vector_size_t row) { return row; }),
      vector1->size(),
      vector1);
  auto rowVector6 = makeRowVector({dictionaryVector});
  cb1 = std::make_shared<VeloxColumnarBatch>(rowVector6);
  ASSERT_NOT_OK(shuffleWriter->write(cb1, memLimit));
  EXPECT_EQ(rowVector6->retainedSize(), shuffleWriter->getInputColumnBytes());
}

} // namespace gluten