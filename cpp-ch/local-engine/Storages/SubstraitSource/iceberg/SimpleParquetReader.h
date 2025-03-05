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
#include <Interpreters/Context_fwd.h>
#include <Storages/SubstraitSource/substrait_fwd.h>

namespace DB
{
class ReadBuffer;
class ParquetReader;
}
namespace local_engine
{

namespace iceberg
{
substraitInputFile fromDeleteFile(const substraitIcebergDeleteFile & deleteFile);
}

class ColumnIndexRowRangesProvider;
class VectorizedParquetRecordReader;

/// we currently use VectorizedParquetRecordReader to read parquet files.
class SimpleParquetReader
{
  std::unique_ptr<DB::ReadBuffer> read_buffer_;
  std::shared_ptr<DB::ParquetReader> reader_;

public:
  explicit SimpleParquetReader(const DB::ContextPtr & context, const substraitInputFile & file_info);
  explicit SimpleParquetReader(const DB::ContextPtr & context, const substraitIcebergDeleteFile & file_info);
  ~SimpleParquetReader();

  DB::Block next() const;
};

}