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

#include <arrow/filesystem/api.h>
#include <arrow/result.h>
#include <arrow/util/range.h>
#include <benchmark/benchmark.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>

#include "BenchmarkUtils.h"
#include "compute/VeloxPlanConverter.h"
#include "jni/exec_backend.h"
#include "utils/exception.h"

class WrapperIterator {
 public:
  explicit WrapperIterator(arrow::RecordBatchVector batches)
      : batches_(std::move(batches)), iter_(batches_.begin()) {}

  std::shared_ptr<arrow::RecordBatch> Next() {
    return iter_ == batches_.cend() ? nullptr : *iter_++;
  }

 private:
  arrow::RecordBatchVector batches_;
  std::vector<std::shared_ptr<arrow::RecordBatch>>::const_iterator iter_;
};

std::shared_ptr<gluten::RecordBatchResultIterator> GetInputIterator(
    const std::string& path) {
  std::unique_ptr<parquet::arrow::FileReader> parquet_reader;
  std::shared_ptr<arrow::RecordBatchReader> rb_reader;
  parquet::ArrowReaderProperties properties = parquet::default_arrow_reader_properties();

  GLUTEN_THROW_NOT_OK(parquet::arrow::FileReader::Make(
      arrow::default_memory_pool(), parquet::ParquetFileReader::OpenFile(path),
      properties, &parquet_reader));
  GLUTEN_THROW_NOT_OK(parquet_reader->GetRecordBatchReader(
      arrow::internal::Iota(parquet_reader->num_row_groups()), &rb_reader));
  GLUTEN_ASSIGN_OR_THROW(auto collect_batches, rb_reader->ToRecordBatches());

  return std::make_shared<gluten::RecordBatchResultIterator>(
      std::make_shared<WrapperIterator>(std::move(collect_batches)));
}

auto BM_HashJoin = [](::benchmark::State& state, const std::string& l_input_file,
                      const std::string& r_input_file) {
  const auto& filePath = getExampleFilePath("hash_join.json");
  auto maybePlan = readFromFile(filePath);
  if (!maybePlan.ok()) {
    state.SkipWithError(maybePlan.status().message().c_str());
  }
  auto plan = std::move(maybePlan).ValueOrDie();

  for (auto _ : state) {
    state.PauseTiming();
    auto backend = gluten::CreateBackend();
    auto l_input_iter = GetInputIterator(l_input_file);
    auto r_input_iter = GetInputIterator(r_input_file);
    state.ResumeTiming();
    backend->ParsePlan(plan->data(), plan->size());
    auto result_iter =
        backend->GetResultIterator({std::move(l_input_iter), std::move(r_input_iter)});

    while (result_iter->HasNext()) {
      std::cout << result_iter->Next()->ToString() << std::endl;
    }
  }
};

auto BM_HashJoinExample = [](::benchmark::State& state) {
  const auto& bm_lineitem = getExampleFilePath(
      "parquet/bm_lineitem/"
      "part-00000-8bd1ea02-5f13-449f-b7ef-e32a0f11583d-c000.snappy.parquet");
  const auto& bm_part = getExampleFilePath(
      "parquet/bm_part/"
      "part-00000-d8bbcbeb-f056-4b7f-8f80-7e5ee7260b9f-c000.snappy.parquet");
  return BM_HashJoin(state, bm_lineitem, bm_part);
};

/**
  If no input files specified, small input files will be used as examples.
  Larger input files can be generated using following commands:

  spark.sql("""
    select cast(l_partkey as double), cast(l_extendedprice as double) from lineitem
  """).write.format("parquet").save("file:///path/to/bm_lineitem")

  spark.sql("""
    select cast(p_partkey as double) from part
  """).write.format("parquet").save("file:///path/to/bm_part")
 */
int main(int argc, char** argv) {
  std::unique_ptr<facebook::velox::memory::MemoryPool> veloxPool =
      facebook::velox::memory::getDefaultScopedMemoryPool();
  InitVeloxBackend(veloxPool.get());
  ::benchmark::Initialize(&argc, argv);
  if (argc < 3) {
    std::cout << "Running example." << std::endl;
    ::benchmark::RegisterBenchmark("hash_join_example", BM_HashJoinExample);
  } else {
    ::benchmark::RegisterBenchmark("hash_join", BM_HashJoin, argv[1], argv[2]);
  }

  ::benchmark::RunSpecifiedBenchmarks();
  ::benchmark::Shutdown();

  return 0;
}
