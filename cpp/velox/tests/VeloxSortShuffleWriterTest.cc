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

#include "tests/VeloxShuffleWriterTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::test;

namespace gluten {

namespace {
class FakeBufferRssClient : public RssClient {
 public:
  FakeBufferRssClient() = default;

  int32_t pushPartitionData(int32_t partitionId, const char* bytes, int64_t size) override {
    receiveTimes_++;
    return size;
  }

  void stop() override {}

  const uint32_t getReceiveTimes() const {
    return receiveTimes_;
  }

 private:
  uint32_t receiveTimes_{0};
};
} // namespace

class VeloxSortShuffleWriterTest : public VeloxShuffleWriterTestBase, public testing::Test {
 protected:
  static void SetUpTestSuite() {
    setUpVeloxBackend();
  }

  static void TearDownTestSuite() {
    tearDownVeloxBackend();
  }

  std::shared_ptr<VeloxShuffleWriter> createShuffleWriter(
      uint32_t numPartitions,
      std::shared_ptr<SortShuffleWriterOptions> writeOptions,
      std::shared_ptr<RssClient> rssClient) {
    auto options = std::make_shared<RssPartitionWriterOptions>();
    auto partitionWriter = std::make_shared<RssPartitionWriter>(
        numPartitions, nullptr, getDefaultMemoryManager(), options, std::move(rssClient));
    GLUTEN_ASSIGN_OR_THROW(
        auto shuffleWriter,
        VeloxSortShuffleWriter::create(
            numPartitions, std::move(partitionWriter), std::move(writeOptions), getDefaultMemoryManager()));
    return shuffleWriter;
  }
};

TEST_F(VeloxSortShuffleWriterTest, pushCompleteRows) {
  auto rssClient = std::make_shared<FakeBufferRssClient>();
  auto writeOptions = std::make_shared<SortShuffleWriterOptions>();
  // Make buffer size smallest to ensure each push only contains one row.
  writeOptions->diskWriteBufferSize = 1;

  auto rowVector = makeRowVector({
      makeFlatVector<StringView>(
          {"alice0",
           "bob1",
           "alice2",
           "bob3",
           "Alice4",
           "Bob5123456789098766notinline",
           "AlicE6",
           "boB7",
           "ALICE8",
           "BOB9"}),
  });
  std::shared_ptr<ColumnarBatch> cb = std::make_shared<VeloxColumnarBatch>(rowVector);

  auto shuffleWriter = createShuffleWriter(1, writeOptions, rssClient);
  auto status = shuffleWriter->write(cb, ShuffleWriter::kMinMemLimit);
  ASSERT_TRUE(shuffleWriter->stop().ok());

  // numRows should equal to push data times in rss client.
  EXPECT_EQ(10, rssClient->getReceiveTimes());
}

} // namespace gluten
