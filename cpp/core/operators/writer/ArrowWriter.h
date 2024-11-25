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

#pragma once

#include <parquet/arrow/writer.h>
#include "memory/ColumnarBatch.h"

namespace gluten {
/**
 * @brief Used to print RecordBatch to a parquet file
 *
 */
class ArrowWriter {
 public:
  explicit ArrowWriter(const std::string& path) : path_(path) {}

  virtual ~ArrowWriter() = default;

  arrow::Status initWriter(arrow::Schema& schema);

  arrow::Status writeInBatches(std::shared_ptr<arrow::RecordBatch> batch);

  arrow::Status closeWriter();

  bool closed() const;

  virtual std::shared_ptr<ColumnarBatch> retrieveColumnarBatch() = 0;

 protected:
  std::unique_ptr<parquet::arrow::FileWriter> writer_;
  std::string path_;
  bool closed_{false};
};
} // namespace gluten
