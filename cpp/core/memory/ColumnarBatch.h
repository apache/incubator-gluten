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
#include "utils/exception.h"

namespace gluten {

class ColumnarBatch {
 public:
  ColumnarBatch(int32_t numColumns, int32_t numRows) : numColumns_(numColumns), numRows_(numRows), exportNanos_(0) {}

  virtual ~ColumnarBatch() = default;

  int32_t getNumColumns() const {
    return numColumns_;
  }

  int32_t getNumRows() const {
    return numRows_;
  }

  virtual std::string getType() const = 0;

  virtual int64_t getBytes() = 0;

  // Will change this columnar batch
  virtual std::shared_ptr<ColumnarBatch> addColumn(int32_t index, std::shared_ptr<ColumnarBatch> col) {
    throw std::runtime_error("Not implement addColumn");
  }

  virtual std::shared_ptr<ArrowArray> exportArrowArray() = 0;

  virtual std::shared_ptr<ArrowSchema> exportArrowSchema() = 0;

  // FIXME: No need to have this virtual function
  virtual void saveToFile(std::shared_ptr<ArrowWriter> writer) = 0;

  virtual int64_t getExportNanos() const {
    return exportNanos_;
  };

  friend std::ostream& operator<<(std::ostream& os, const ColumnarBatch& columnarBatch) {
    return os << "NumColumns: " << std::to_string(columnarBatch.getNumColumns())
              << "NumRows: " << std::to_string(columnarBatch.getNumRows());
  }

 private:
  int32_t numColumns_;
  int32_t numRows_;

 protected:
  int64_t exportNanos_;
};

class ArrowColumnarBatch final : public ColumnarBatch {
 public:
  explicit ArrowColumnarBatch(std::shared_ptr<arrow::RecordBatch> batch)
      : ColumnarBatch(batch->num_columns(), batch->num_rows()), batch_(std::move(batch)) {}

  std::string getType() const override {
    return "arrow";
  }

  int64_t getBytes() override {
    throw gluten::GlutenException("Not implemented GetBytes for ArrowColumnarBatch");
  }

  arrow::RecordBatch* getRecordBatch() const {
    return batch_.get();
  }

  std::shared_ptr<ArrowSchema> exportArrowSchema() override {
    auto cSchema = std::make_shared<ArrowSchema>();
    GLUTEN_THROW_NOT_OK(arrow::ExportSchema(*batch_->schema(), cSchema.get()));
    return cSchema;
  }

  std::shared_ptr<ArrowArray> exportArrowArray() override {
    auto cArray = std::make_shared<ArrowArray>();
    GLUTEN_THROW_NOT_OK(arrow::ExportRecordBatch(*batch_, cArray.get()));
    return cArray;
  }

  void saveToFile(std::shared_ptr<ArrowWriter> writer) override {
    GLUTEN_THROW_NOT_OK(writer->initWriter(*(batch_->schema().get())));
    GLUTEN_THROW_NOT_OK(writer->writeInBatches(batch_));
  }

 private:
  std::shared_ptr<arrow::RecordBatch> batch_;
};

class ArrowCStructColumnarBatch final : public ColumnarBatch {
 public:
  ArrowCStructColumnarBatch(std::unique_ptr<ArrowSchema> cSchema, std::unique_ptr<ArrowArray> cArray)
      : ColumnarBatch(cArray->n_children, cArray->length) {
    ArrowSchemaMove(cSchema.get(), cSchema_.get());
    ArrowArrayMove(cArray.get(), cArray_.get());
  }

  ~ArrowCStructColumnarBatch() override {
    ArrowSchemaRelease(cSchema_.get());
    ArrowArrayRelease(cArray_.get());
  }

  std::string getType() const override {
    return "arrow_array";
  }

  int64_t getBytes() override {
    int64_t bytes = cArray_->n_buffers;
    for (int64_t i = 0; i < cArray_->n_children; ++i) {
      bytes += cArray_->children[i]->n_buffers;
    }
    return bytes;
  }

  std::shared_ptr<ArrowSchema> exportArrowSchema() override {
    return cSchema_;
  }

  std::shared_ptr<ArrowArray> exportArrowArray() override {
    return cArray_;
  }

  void saveToFile(std::shared_ptr<ArrowWriter> writer) override {
    GLUTEN_ASSIGN_OR_THROW(auto rb, arrow::ImportRecordBatch(cArray_.get(), cSchema_.get()));
    GLUTEN_THROW_NOT_OK(writer->initWriter(*(rb->schema().get())));
    GLUTEN_THROW_NOT_OK(writer->writeInBatches(rb));

    arrow::Status status = arrow::ExportRecordBatch(*rb, cArray_.get(), cSchema_.get());
    if (!status.ok()) {
      throw std::runtime_error("Failed to export from Arrow record batch");
    }
  }

 private:
  std::shared_ptr<ArrowSchema> cSchema_ = std::make_shared<ArrowSchema>();
  std::shared_ptr<ArrowArray> cArray_ = std::make_shared<ArrowArray>();
};

} // namespace gluten
