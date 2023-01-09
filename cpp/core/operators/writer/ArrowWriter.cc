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

#include "ArrowWriter.h"

#include "arrow/io/file.h"
#include "arrow/table.h"
#include "arrow/util/type_fwd.h"

arrow::Status ArrowWriter::initWriter(arrow::Schema& schema) {
  if (writer_ != nullptr) {
    return arrow::Status::OK();
  }
  using parquet::ArrowWriterProperties;
  using parquet::WriterProperties;
  // Choose compression
  std::shared_ptr<WriterProperties> props =
      WriterProperties::Builder().compression(arrow::Compression::SNAPPY)->build();

  // Opt to store Arrow schema for easier reads back into Arrow
  std::shared_ptr<ArrowWriterProperties> arrow_props = ArrowWriterProperties::Builder().store_schema()->build();

  // Create a writer
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open(path_));
  ARROW_RETURN_NOT_OK(
      parquet::arrow::FileWriter::Open(schema, arrow::default_memory_pool(), outfile, props, arrow_props, &writer_));
  return arrow::Status::OK();
}

arrow::Status ArrowWriter::writeInBatches(std::shared_ptr<arrow::RecordBatch> batch) {
  // Write each batch as a row_group
  ARROW_ASSIGN_OR_RAISE(auto table, arrow::Table::FromRecordBatches(batch->schema(), {batch}));
  ARROW_RETURN_NOT_OK(writer_->WriteTable(*table.get(), batch->num_rows()));
  return arrow::Status::OK();
}

arrow::Status ArrowWriter::closeWriter() {
  // Write file footer and close
  if (writer_ != nullptr) {
    ARROW_RETURN_NOT_OK(writer_->Close());
  }
  return arrow::Status::OK();
}
