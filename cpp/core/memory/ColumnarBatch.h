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

#include <memory>

#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/record_batch.h"
#include "operators/writer/ArrowWriter.h"
#include "utils/ArrowStatus.h"
#include "utils/exception.h"

namespace gluten {

class ColumnarBatch {
 public:
  ColumnarBatch(int32_t numColumns, int32_t numRows);

  virtual ~ColumnarBatch() = default;

  int32_t numColumns() const;

  int32_t numRows() const;

  virtual std::string getType() const = 0;

  virtual int64_t numBytes() = 0;

  virtual std::shared_ptr<ArrowArray> exportArrowArray() = 0;

  virtual std::shared_ptr<ArrowSchema> exportArrowSchema() = 0;

  virtual int64_t getExportNanos() const;
  ;

  friend std::ostream& operator<<(std::ostream& os, const ColumnarBatch& columnarBatch);

 private:
  int32_t numColumns_;
  int32_t numRows_;

 protected:
  int64_t exportNanos_;
};

class ArrowColumnarBatch final : public ColumnarBatch {
 public:
  explicit ArrowColumnarBatch(std::shared_ptr<arrow::RecordBatch> batch);

  std::string getType() const override;

  int64_t numBytes() override;

  arrow::RecordBatch* getRecordBatch() const;

  std::shared_ptr<ArrowSchema> exportArrowSchema() override;

  std::shared_ptr<ArrowArray> exportArrowArray() override;

 private:
  std::shared_ptr<arrow::RecordBatch> batch_;
};

class ArrowCStructColumnarBatch final : public ColumnarBatch {
 public:
  ArrowCStructColumnarBatch(std::unique_ptr<ArrowSchema> cSchema, std::unique_ptr<ArrowArray> cArray);

  ~ArrowCStructColumnarBatch() override;

  std::string getType() const override;

  int64_t numBytes() override;

  std::shared_ptr<ArrowSchema> exportArrowSchema() override;

  std::shared_ptr<ArrowArray> exportArrowArray() override;

 private:
  std::shared_ptr<ArrowSchema> cSchema_ = std::make_shared<ArrowSchema>();
  std::shared_ptr<ArrowArray> cArray_ = std::make_shared<ArrowArray>();
};

/**
 * A columnar batch implementations that creates a view on top of a couple of child batches.
 * Fields in the child batches will be organized horizontally in the parent batch.
 */
class CompositeColumnarBatch final : public ColumnarBatch {
 public:
  static std::shared_ptr<ColumnarBatch> create(std::vector<std::shared_ptr<ColumnarBatch>> batches);

  std::string getType() const override;

  int64_t numBytes() override;

  std::shared_ptr<ArrowArray> exportArrowArray() override;

  std::shared_ptr<ArrowSchema> exportArrowSchema() override;

  const std::vector<std::shared_ptr<ColumnarBatch>>& getBatches() const;

 private:
  explicit CompositeColumnarBatch(
      int32_t numColumns,
      int32_t numRows,
      std::vector<std::shared_ptr<ColumnarBatch>> batches);

  // We use ArrowColumnarBatch as the way to compose columnar batches
  void ensureUnderlyingBatchCreated();

  std::vector<std::shared_ptr<ColumnarBatch>> batches_;
  std::shared_ptr<ColumnarBatch> compositeBatch_ = nullptr;
};

} // namespace gluten
