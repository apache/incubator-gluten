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
#include <Core/Block.h>
#include <Interpreters/ActionsDAG.h>
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
SubstraitInputFile fromDeleteFile(const SubstraitIcebergDeleteFile & deleteFile);
}

class ColumnIndexRowRangesProvider;
class VectorizedParquetRecordReader;

/// we currently use VectorizedParquetRecordReader to read parquet files.
class SimpleParquetReader
{
    std::unique_ptr<DB::ReadBuffer> read_buffer_arrow_;
    std::unique_ptr<DB::ReadBuffer> read_buffer_reader_;
    std::shared_ptr<DB::ParquetReader> reader_;

public:
    SimpleParquetReader(
        const DB::ContextPtr & context,
        const SubstraitInputFile & file_info,
        DB::Block header = {},
        const std::optional<DB::ActionsDAG> & filter = std::nullopt);
    SimpleParquetReader(
        const DB::ContextPtr & context,
        const SubstraitIcebergDeleteFile & file_info,
        DB::Block header = {},
        const std::optional<DB::ActionsDAG> & filter = std::nullopt);
    ~SimpleParquetReader();

    DB::Block next() const;
};

}