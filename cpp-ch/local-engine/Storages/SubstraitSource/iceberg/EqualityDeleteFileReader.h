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

#include <Core/Block.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <substrait/plan.pb.h>

namespace local_engine
{
class ColumnIndexRowRangesProvider;
class VectorizedParquetRecordReader;

using substraitInputFile = substrait::ReadRel::LocalFiles::FileOrFiles;
using substraitIcebergDeleteFile = substrait::ReadRel::LocalFiles::FileOrFiles::IcebergReadOptions::DeleteFile;
using IcebergReadOptions = substrait::ReadRel::LocalFiles::FileOrFiles::IcebergReadOptions;

// TODO: move to other cpp ?

/// we currently use this class to read parquet files.
class SimpleParquetReader
{
    std::unique_ptr<DB::ReadBuffer> read_buffer_;
    DB::Block fileHeader_;
    std::unique_ptr<ColumnIndexRowRangesProvider> provider_;
    std::unique_ptr<VectorizedParquetRecordReader> reader_;

public:
    explicit SimpleParquetReader(const DB::ContextPtr & context, const substraitInputFile & file_info);
    ~SimpleParquetReader();

    DB::Block next() const;
};

namespace iceberg
{

class EqualityDeleteFileReader
{
    SimpleParquetReader reader_;

public:
    explicit EqualityDeleteFileReader(const DB::ContextPtr & context, const substraitIcebergDeleteFile & deleteFile);
    ~EqualityDeleteFileReader() = default;
    void readDeleteValues(DB::ASTs & expressionInputs) const;
};

}
}
