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

#include <arrow/record_batch.h>
#include <gtest/gtest.h>

#include "jni/JniErrors.h"
#include "memory/ArrowMemoryPool.h"
#include "operators/c2r/ArrowColumnarToRowConverter.h"
#include "tests/TestUtils.h"

using namespace arrow;

namespace gluten {
class ColumnarToRowTest : public ::testing::Test {};

void testConvertBatch(std::shared_ptr<RecordBatch> input_batch) {
  auto arrowPool = GetDefaultWrappedArrowMemoryPool();
  auto converter = std::make_shared<ArrowColumnarToRowConverter>(input_batch, arrowPool);
  JniAssertOkOrThrow(
      converter->Init(),
      "Native convert columnar to row: Init "
      "ColumnarToRowConverter failed");
  JniAssertOkOrThrow(converter->Write(), "Native convert columnar to row: ColumnarToRowConverter write failed");
}

TEST_F(ColumnarToRowTest, decimal) {
  {
    std::vector<std::shared_ptr<Field>> fields = {field("f_decimal128", decimal(10, 2))};

    auto schema = arrow::schema(fields);
    std::shared_ptr<RecordBatch> input_batch;
    const std::vector<std::string> input_data = {R"(["-1.01", "2.95"])"};
    MakeInputBatch(input_data, schema, &input_batch);
    testConvertBatch(input_batch);
  }

  {
    auto f2 = {field("f2", timestamp(TimeUnit::MICRO))};
    auto schema = arrow::schema(f2);
    std::shared_ptr<RecordBatch> input_batch;
    const std::vector<std::string> input_data = {R"(["1970-01-01","2000-02-29","3989-07-14","1900-02-28"])"};
    MakeInputBatch(input_data, schema, &input_batch);
    testConvertBatch(input_batch);
  }
}
} // namespace gluten
