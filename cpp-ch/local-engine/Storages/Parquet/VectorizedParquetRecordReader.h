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

#include "Common/assert_cast.h"


#include <config.h>

#if USE_PARQUET

#include <Formats/FormatSettings.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Storages/Parquet/ParquetReadState.h>
#include <parquet/arrow/reader_internal.h>
#include <parquet/arrow/schema.h>

namespace parquet
{
struct PageLocation;
}
namespace DB
{
class DataTypeNullable;
class Block;
}

namespace arrow
{
class ChunkedArray;
}
namespace local_engine
{
class RowRanges;
using ReadRanges = std::vector<arrow::io::ReadRange>;
using ReadSequence = std::vector<int64_t>;
using PageReaderPtr = std::unique_ptr<parquet::PageReader>;
class VectorizedParquetBlockInputFormat;
class ColumnIndexRowRangesProvider;
class ParquetReadState1;
class ParquetReadState2;
using ParquetReadState = ParquetReadState2;
using ParquetReadStatePtr = std::unique_ptr<ParquetReadState>;
using ColumnChunkPageRead = std::pair<PageReaderPtr, ParquetReadStatePtr>;


std::shared_ptr<parquet::ArrowInputStream>
getStream(arrow::io::RandomAccessFile & reader, const std::vector<arrow::io::ReadRange> & ranges);

class ParquetReadState1
{
public:
    using SkipFunc = std::function<int64_t(int64_t)>;
    using ReadFunc = std::function<int64_t(int64_t)>;

private:
    ReadSequence read_sequence_;
    int index_ = 0;
    ReadFunc read_func_;
    SkipFunc skip_func_;

    void advance(const int64_t read_or_skip)
    {
        assert(hasMoreRead());
        assert(read_sequence_[index_] >= read_or_skip);
        read_sequence_[index_] -= read_or_skip;
        if (read_sequence_[index_] == 0)
            ++index_;
    }

    int64_t currentRead() const
    {
        assert(hasMoreRead());
        return read_sequence_[index_];
    }

    void skip(const int64_t skip)
    {
        assert(skip > 0);
        advance(-skip);
    }

    void read(const int64_t read)
    {
        assert(read > 0);
        advance(read);
    }

    int64_t SkipRecords(int64_t num_records) const
    {
        chassert(skip_func_);
        chassert(num_records > 0);
        return skip_func_(num_records);
    }

    int64_t ReadRecords(int64_t num_records) const
    {
        chassert(read_func_);
        chassert(num_records > 0);
        return read_func_(num_records);
    }

public:
    explicit ParquetReadState1(const ReadSequence & read_sequence) : read_sequence_(read_sequence) { }

    void setReadFunc(ReadFunc read_func) { read_func_ = std::move(read_func); }
    void setSkipFunc(SkipFunc skip_func) { skip_func_ = std::move(skip_func); }

    bool hasMoreRead() const
    {
        if (read_sequence_.back() < 0)
            return index_ < read_sequence_.size() - 1;
        return index_ < read_sequence_.size();
    }

    void skipLastRecord()
    {
        assert(!hasMoreRead());
        if (read_sequence_.back() < 0)
        {
            assert(index_ == read_sequence_.size() - 1);
            const int64_t skip = read_sequence_.back();
            [[maybe_unused]] const int64_t records_skipped = SkipRecords(-skip);
            assert(records_skipped == -skip);
            read_sequence_.back() -= skip;
            index_++;
        }
    }

    int64_t doRead(int64_t batch_size)
    {
        while (hasMoreRead() && batch_size > 0)
        {
            const int64_t readNumber = currentRead();
            if (readNumber < 0)
            {
                const int64_t records_skipped = SkipRecords(-readNumber);
                assert(records_skipped == -readNumber);
                skip(records_skipped);
                assert(hasMoreRead());
            }
            else
            {
                const int64_t readBatch = std::min(batch_size, readNumber);
                const int64_t records_read = ReadRecords(readBatch);
                assert(records_read == readBatch);
                batch_size -= records_read;
                read(records_read);
                if (!hasMoreRead())
                    break;
            }
        }
        return batch_size;
    }
};

class ParquetFileReaderExt
{
    /// Members
    std::shared_ptr<::arrow::io::RandomAccessFile> source_;
    int64_t source_size_;
    std::unique_ptr<parquet::ParquetFileReader> file_reader_;
    const DB::FormatSettings & format_settings_;
    const ColumnIndexRowRangesProvider & row_ranges_provider_;

    PageReaderPtr createPageReader(
        const std::shared_ptr<parquet::ArrowInputStream> & input_stream, const parquet::ColumnChunkMetaData & column_metadata) const;

    ColumnChunkPageRead
    nextRowGroup(const RowRanges & row_ranges, int32_t row_group_index, int32_t column_index, const std::string & column_name) const;

public:
    ParquetFileReaderExt(
        const std::shared_ptr<arrow::io::RandomAccessFile> & source,
        std::unique_ptr<parquet::ParquetFileReader> parquetFileReader,
        const ColumnIndexRowRangesProvider & row_ranges_provider,
        const DB::FormatSettings & format_settings);
    std::optional<ColumnChunkPageRead> nextRowGroup(int32_t row_group_index, int32_t column_index, const std::string & column_name) const;
    parquet::ParquetFileReader * fileReader() const { return file_reader_.get(); }
    std::shared_ptr<parquet::FileMetaData> fileMeta() const { return file_reader_->metadata(); }

    // UT
    PageReaderPtr
    createPageReader(const std::shared_ptr<parquet::ArrowInputStream> & input_stream, int32_t row_group_index, int32_t column_index) const;
};

class PageIterator final : public parquet::arrow::FileColumnIterator
{
    ParquetFileReaderExt & reader_ext_;

public:
    PageIterator(const int column_index, ParquetFileReaderExt & reader_ext, const std::vector<Int32> & row_groups)
        : FileColumnIterator(column_index, reader_ext.fileReader(), row_groups), reader_ext_(reader_ext)
    {
    }

    ~PageIterator() override = default;

    std::optional<ColumnChunkPageRead> nextRowGroup();
};

class VectorizedColumnReader
{
    std::shared_ptr<arrow::Field> arrow_field_;
    PageIterator input_;
    std::shared_ptr<parquet::internal::RecordReader> record_reader_;
    ParquetReadStatePtr read_state_;

    void nextRowGroup();
    void setPageReader(PageReaderPtr reader, ParquetReadStatePtr read_state);

    parquet::ColumnReader & columnReader() const
    {
        chassert(record_reader_);
        parquet::ColumnReader * col_reader = dynamic_cast<parquet::ColumnReader *>(record_reader_.get());
        chassert(col_reader);
        return *col_reader;
    }

public:
    VectorizedColumnReader(const parquet::arrow::SchemaField & field, ParquetFileReaderExt & reader, const std::vector<Int32> & row_groups);
    const std::string & columnName() const { return arrow_field_->name(); }
    std::shared_ptr<arrow::Field> arrowField() const { return arrow_field_; }
    bool hasMoreRead() const { return read_state_ && read_state_->hasMoreRead(); }
    std::shared_ptr<arrow::ChunkedArray> readBatch(int64_t batch_size);
};

class VectorizedParquetRecordReader
{
    const DB::Block parquet_header_;
    const DB::FormatSettings format_settings_;
    DB::ArrowColumnToCHColumn arrow_column_to_ch_column_;

    std::unique_ptr<ParquetFileReaderExt> file_reader_;

    // parquet::arrow::SchemaManifest manifest_;
    /// columns to read from Parquet file.
    std::vector<VectorizedColumnReader> column_readers_;

public:
    static parquet::arrow::SchemaManifest createSchemaManifest(const parquet::FileMetaData & metadata);

    VectorizedParquetRecordReader(const DB::Block & header, const DB::FormatSettings & format_settings);
    ~VectorizedParquetRecordReader() = default;

    bool initialize(
        const std::shared_ptr<arrow::io::RandomAccessFile> & arrow_file,
        const ColumnIndexRowRangesProvider & row_ranges_provider,
        const std::shared_ptr<parquet::FileMetaData> & metadata = nullptr);
    DB::Chunk nextBatch();

    bool initialized() const { return file_reader_ != nullptr; }

    void reset()
    {
        column_readers_.clear();
        file_reader_.reset();
    }

    const DB::FormatSettings & formatSettings() const { return format_settings_; }
};

/// InputFormat

class VectorizedParquetBlockInputFormat final : public DB::IInputFormat
{
    std::atomic<int> is_stopped{0};
    VectorizedParquetRecordReader record_reader_;
    const ColumnIndexRowRangesProvider & row_ranges_provider_;

protected:
    void onCancel() noexcept override { is_stopped = 1; }

public:
    VectorizedParquetBlockInputFormat(
        DB::ReadBuffer & in_,
        const DB::SharedHeader & header_,
        const ColumnIndexRowRangesProvider & row_ranges_provider,
        const DB::FormatSettings & format_settings);
    String getName() const override { return "VectorizedParquetBlockInputFormat"; }
    void resetParser() override;

private:
    DB::Chunk read() override;
};
}
#endif
