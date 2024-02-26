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
#include <config.h>

#if USE_PARQUET

#include <cstddef>
#include <Formats/FormatSettings.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <parquet/arrow/reader_internal.h>
#include <parquet/arrow/schema.h>
#include <Storages/Parquet/ArrowUtils.h>

namespace DB
{
class Block;
}

namespace arrow
{
class ChunkedArray;
}
namespace local_engine2
{
class VectorizedParquetRecordReader;
class VectorizedParquetBlockInputFormat;

class VectorizedColumnReader
{
private:
    std::shared_ptr<arrow::Field> arrowField_;
    parquet::arrow::FileColumnIterator input_;
    std::shared_ptr<parquet::internal::RecordReader> record_reader_;
    void NextRowGroup();
    friend class VectorizedParquetRecordReader;

public:
    VectorizedColumnReader(
        const parquet::arrow::SchemaField & field, parquet::ParquetFileReader * reader, const std::vector<int32_t> & row_groups);

    void prepareRead(const int64_t batch_size) const
    {
        record_reader_->Reset();
        record_reader_->Reserve(batch_size);
    }

    using ReadNextGroup = std::function<void()>;
    void readBatch(size_t batch_size, const ReadNextGroup & read_next_group) const;

    std::shared_ptr<arrow::ChunkedArray> finishRead() const
    {
        std::shared_ptr<arrow::ChunkedArray> result;
        THROW_ARROW_NOT_OK(parquet::arrow::TransferColumnData(record_reader_.get(), arrowField_, input_.descr(), local_engine::default_arrow_pool(), &result));
        return result;
    }
};

class VectorizedParquetRecordReader
{
private:
    const DB::FormatSettings format_settings_;
    DB::ArrowColumnToCHColumn arrowColumnToCHColumn_;

    std::unique_ptr<parquet::ParquetFileReader> parquetFileReader_;
    parquet::arrow::SchemaManifest manifest_;
    /// columns to read from Parquet file.
    std::vector<VectorizedColumnReader> columnVectors_;

    friend class VectorizedParquetBlockInputFormat;

public:
    VectorizedParquetRecordReader(const DB::Block & header, const DB::FormatSettings & format_settings);
    ~VectorizedParquetRecordReader() = default;
    void initialize(
        const DB::Block & header,
        const std::shared_ptr<arrow::io::RandomAccessFile> & arrow_file,
        const std::shared_ptr<parquet::FileMetaData> & metadata = nullptr);
    DB::Chunk nextBatch();

    bool initialized() { return parquetFileReader_ != nullptr; }

    void reset()
    {
        columnVectors_.clear();
        parquetFileReader_.reset();
    }
};


/// InputFormat

class VectorizedParquetBlockInputFormat : public DB::IInputFormat
{
private:
    std::atomic<int> is_stopped{0};
    DB::BlockMissingValues block_missing_values;
    VectorizedParquetRecordReader recordReader_;

protected:
    void onCancel() override { is_stopped = 1; }

public:
    VectorizedParquetBlockInputFormat(DB::ReadBuffer & in_, const DB::Block & header_, const DB::FormatSettings & format_settings_);

    String getName() const override { return "VectorizedParquetBlockInputFormat"; }
    void resetParser() override;
    const DB::BlockMissingValues & getMissingValues() const override;

private:
    DB::Chunk read() override;
};

}
#endif
