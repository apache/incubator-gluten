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
#include "shuffle/RoundRobinPartitioner.h"
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <numeric>

namespace gluten {
class RoundRobinPartitionerTest : public ::testing::Test {
 protected:
  void prepareData(int numPart, int seed) {
    partitioner_ = std::make_shared<RoundRobinPartitioner>(numPart, seed);
    row2Partition_.clear();
    partition2RowCount_.clear();
    partition2RowCount_.resize(numPart);
  }

  void checkResult(const std::vector<uint32_t>& expectRow2Part, const std::vector<uint16_t>& expectPart2RowCount)
      const {
    ASSERT_EQ(row2Partition_, expectRow2Part);
    ASSERT_EQ(partition2RowCount_, expectPart2RowCount);
  }

  void traceCheckResult(const std::vector<uint32_t>& expectRow2Part, const std::vector<uint16_t>& expectPart2RowCount)
      const {
    toString(expectRow2Part, "expectRow2Part");
    toString(expectPart2RowCount, "expectPart2RowCount");
    toString(row2Partition_, "row2Partition_");
    toString(partition2RowCount_, "partition2RowCount_");
    ASSERT_EQ(row2Partition_, expectRow2Part);
    ASSERT_EQ(partition2RowCount_, expectPart2RowCount);
  }

  template <typename T>
  void toString(const std::vector<T>& vec, const std::string& name) const {
    std::stringstream ss;
    ss << name << " = [";
    std::copy(vec.cbegin(), vec.cend(), std::ostream_iterator<T>(ss, ","));
    ss << " ]";
    LOG(INFO) << ss.str();
  }

  int32_t getPidSelection() const {
    return partitioner_->pidSelection_;
  }

  std::vector<uint32_t> row2Partition_;
  std::vector<uint16_t> partition2RowCount_;
  std::shared_ptr<RoundRobinPartitioner> partitioner_;
};

TEST_F(RoundRobinPartitionerTest, TestInit) {
  int numPart = 2;
  prepareData(numPart, 3);
  ASSERT_NE(partitioner_, nullptr);
  int32_t pidSelection = getPidSelection();
  ASSERT_EQ(pidSelection, 1);
}

TEST_F(RoundRobinPartitionerTest, TestComoputeNormal) {
  // numRows equal numPart
  {
    int numPart = 10;
    prepareData(numPart, 0);
    int numRows = 10;
    ASSERT_TRUE(partitioner_->compute(nullptr, numRows, row2Partition_, partition2RowCount_).ok());
    ASSERT_EQ(getPidSelection(), 0);
    std::vector<uint32_t> row2Part(numRows);
    std::iota(row2Part.begin(), row2Part.end(), 0);
    checkResult(row2Part, std::vector<uint16_t>(numPart, 1));
  }

  // numRows less than numPart
  {
    int numPart = 10;
    prepareData(numPart, 0);
    int numRows = 8;
    ASSERT_TRUE(partitioner_->compute(nullptr, numRows, row2Partition_, partition2RowCount_).ok());
    ASSERT_EQ(getPidSelection(), 8);
    std::vector<uint32_t> row2Part(numRows);
    std::iota(row2Part.begin(), row2Part.end(), 0);
    std::vector<uint16_t> part2RowCount(numPart, 0);
    std::fill_n(part2RowCount.begin(), numRows, 1);
    checkResult(row2Part, part2RowCount);
  }

  // numRows greater than numPart
  {
    int numPart = 10;
    prepareData(numPart, 0);
    int numRows = 12;
    ASSERT_TRUE(partitioner_->compute(nullptr, numRows, row2Partition_, partition2RowCount_).ok());
    ASSERT_EQ(getPidSelection(), 2);
    std::vector<uint32_t> row2Part(numRows);
    std::generate_n(row2Part.begin(), numRows, [n = 0, numPart]() mutable { return (n++) % numPart; });
    std::vector<uint16_t> part2RowCount(numPart, 1);
    std::fill_n(part2RowCount.begin(), numRows - numPart, 2);
    checkResult(row2Part, part2RowCount);
  }

  // numRows greater than 2*numPart
  {
    int numPart = 10;
    prepareData(numPart, 0);
    int numRows = 22;
    ASSERT_TRUE(partitioner_->compute(nullptr, numRows, row2Partition_, partition2RowCount_).ok());
    ASSERT_EQ(getPidSelection(), 2);
    std::vector<uint32_t> row2Part(numRows);
    std::generate_n(row2Part.begin(), numRows, [n = 0, numPart]() mutable { return (n++) % numPart; });
    std::vector<uint16_t> part2RowCount(numPart, 2);
    std::fill_n(part2RowCount.begin(), numRows - 2 * numPart, 3);
    checkResult(row2Part, part2RowCount);
  }
}

TEST_F(RoundRobinPartitionerTest, TestComoputeContinuous) {
  int numPart = 10;
  prepareData(numPart, 0);

  {
    int numRows = 8;
    ASSERT_TRUE(partitioner_->compute(nullptr, numRows, row2Partition_, partition2RowCount_).ok());
    ASSERT_EQ(getPidSelection(), 8);
    std::vector<uint32_t> row2Part(numRows);
    std::generate_n(row2Part.begin(), numRows, [n = 0, numPart]() mutable { return (n++) % numPart; });
    std::vector<uint16_t> part2RowCount(numPart, 0);
    std::fill_n(part2RowCount.begin(), numRows, 1);
    checkResult(row2Part, part2RowCount);
  }

  {
    int numRows = 10;
    ASSERT_TRUE(partitioner_->compute(nullptr, numRows, row2Partition_, partition2RowCount_).ok());
    ASSERT_EQ(getPidSelection(), 8);
    std::vector<uint32_t> row2Part(numRows);
    std::generate_n(row2Part.begin(), numRows, [n = 8, numPart]() mutable { return (n++) % numPart; });
    checkResult(row2Part, std::vector<uint16_t>(numPart, 1));
  }

  {
    int numRows = 12;
    ASSERT_TRUE(partitioner_->compute(nullptr, numRows, row2Partition_, partition2RowCount_).ok());
    ASSERT_EQ(getPidSelection(), 0);
    std::vector<uint32_t> row2Part(numRows);
    std::generate_n(row2Part.begin(), numRows, [n = 8, numPart]() mutable { return (n++) % numPart; });
    std::vector<uint16_t> part2RowCount(numPart, 1);
    std::fill_n(part2RowCount.begin() + 8, numRows - numPart, 2);
    checkResult(row2Part, part2RowCount);
  }

  {
    int numRows = 22;
    ASSERT_TRUE(partitioner_->compute(nullptr, numRows, row2Partition_, partition2RowCount_).ok());
    ASSERT_EQ(getPidSelection(), 2);
    std::vector<uint32_t> row2Part(numRows);
    std::generate_n(row2Part.begin(), numRows, [n = 0, numPart]() mutable { return (n++) % numPart; });
    std::vector<uint16_t> part2RowCount(numPart, 2);
    std::fill_n(part2RowCount.begin(), numRows - 2 * numPart, 3);
    checkResult(row2Part, part2RowCount);
  }
}

} // namespace gluten
