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

#include <Formats/FormatSettings.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Storages/Parquet/ColumnIndexFilter.h>
#include <parquet/arrow/reader_internal.h>
#include <parquet/arrow/schema.h>

namespace parquet
{
struct PageLocation;
}
namespace DB
{
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
using ColumnReadState = std::pair<ReadRanges, ReadSequence>;
using ColumnChunkPageRead = std::pair<std::unique_ptr<parquet::PageReader>, ReadSequence>;
using ColumnChunkPageReadStore = std::unordered_map<int32_t, ColumnChunkPageRead>;
using ColumnChunkPageReadStorePtr = std::unique_ptr<ColumnChunkPageReadStore>;

class ColumnIndexFilter;
class VectorizedParquetRecordReader;
class VectorizedParquetBlockInputFormat;

ColumnReadState buildAllRead(const int64_t rg_count, const arrow::io::ReadRange & chunk_range);
ColumnReadState buildRead(
    int64_t rg_count,
    const arrow::io::ReadRange & chunk_range,
    const std::vector<parquet::PageLocation> & page_locations,
    const RowRanges & row_ranges);
std::shared_ptr<parquet::ArrowInputStream>
getStream(arrow::io::RandomAccessFile & reader, const std::vector<arrow::io::ReadRange> & ranges);

class ParquetReadState
{
    ReadSequence read_sequence_;
    int index_ = 0;

    void advance(const int64_t read_or_skip)
    {
        assert(hasMoreRead());
        assert(read_sequence_[index_] >= read_or_skip);
        read_sequence_[index_] -= read_or_skip;
        if (read_sequence_[index_] == 0)
            ++index_;
    }

public:
    explicit ParquetReadState(const ReadSequence & read_sequence) : read_sequence_(read_sequence) { }
    int64_t currentRead() const
    {
        assert(hasMoreRead());
        return read_sequence_[index_];
    }

    std::optional<int64_t> hasLastSkip() const
    {
        assert(!hasMoreRead());
        if (read_sequence_.back() < 0)
            return read_sequence_.back();
        return std::nullopt;
    }

    bool hasMoreRead() const
    {
        if (read_sequence_.back() < 0)
            return index_ < read_sequence_.size() - 1;
        return index_ < read_sequence_.size();
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
};

class ParquetFileReaderExt
{
    using BuildRead
        = std::function<void(int32_t, const arrow::io::ReadRange & col_range, ReadRanges & read_ranges, ReadSequence & read_infos)>;

    std::shared_ptr<::arrow::io::RandomAccessFile> source_;
    int64_t source_size_;
    std::unique_ptr<parquet::ParquetFileReader> fileReader_;
    int32_t current_row_group_ = 0;
    std::vector<int32_t> row_groups_;
    std::vector<std::unique_ptr<RowRanges>> row_group_row_ranges_;
    std::vector<std::unique_ptr<ColumnIndexStore>> row_group_column_index_stores_;
    std::vector<std::unique_ptr<parquet::RowGroupMetaData>> row_group_metas_;
    std::vector<std::shared_ptr<parquet::RowGroupPageIndexReader>> row_group_index_readers_;
    std::shared_ptr<ColumnIndexFilter> column_index_filter_;
    std::unordered_set<int32_t> column_indices_;

    const RowRanges & getRowRanges(int row_group_index);
    const ColumnIndexStore & getColumnIndexStore(int row_group_index);

    bool advanceRowGroup()
    {
        if (current_row_group_ == row_groups_.size())
            return false;
        // update the current block
        ++current_row_group_;
        return true;
    }

    ColumnChunkPageReadStorePtr readFilteredRowGroups(
        const parquet::RowGroupMetaData & rg, const RowRanges & row_ranges, const ColumnIndexStore & column_index_store) const;
    ColumnChunkPageReadStorePtr readRowGroups(const parquet::RowGroupMetaData & rg) const;
    ColumnChunkPageReadStorePtr readRowGroupsBase(const parquet::RowGroupMetaData & rg, const BuildRead & build_read) const;

    bool canPruningPage() const
    {
        assert(hasMoreRead());
        return column_index_filter_ && row_group_index_readers_[current_row_group_];
    }

public:
    ParquetFileReaderExt(
        const std::shared_ptr<::arrow::io::RandomAccessFile> & source,
        std::unique_ptr<parquet::ParquetFileReader> parquetFileReader,
        const std::shared_ptr<ColumnIndexFilter> & column_index_filter,
        const std::vector<int32_t> & row_groups,
        const std::vector<int32_t> & column_indices);
    ColumnChunkPageReadStorePtr readFilteredRowGroups();
    bool hasMoreRead() const { return current_row_group_ < row_groups_.size(); }
};

class VectorizedColumnReader
{
    int32_t column_index_;
    std::shared_ptr<arrow::Field> field_;
    const parquet::ColumnDescriptor * descr_;
    std::shared_ptr<parquet::internal::RecordReader> record_reader_;
    std::unique_ptr<ParquetReadState> read_state_;
    friend class VectorizedParquetRecordReader;

public:
    VectorizedColumnReader(int32_t column_index, const std::shared_ptr<arrow::Field> & field, const parquet::ColumnDescriptor * descr);
    void SetPageReader(std::unique_ptr<parquet::PageReader> reader, const ReadSequence & read_sequence);
    void prepareRead(int64_t batch_size) const;
    int64_t readBatch(int64_t batch_size) const;
    std::shared_ptr<arrow::ChunkedArray> finishRead() const;
    bool hasMoreRead() const { return read_state_ && read_state_->hasMoreRead(); }
    int32_t column_index() const { return column_index_; }
};

class VectorizedParquetRecordReader
{
    const DB::FormatSettings format_settings_;
    DB::ArrowColumnToCHColumn arrowColumnToCHColumn_;

    std::unique_ptr<ParquetFileReaderExt> parquetFileReader_;

    // parquet::arrow::SchemaManifest manifest_;
    /// columns to read from Parquet file.
    std::vector<VectorizedColumnReader> columnVectors_;
    friend class VectorizedParquetBlockInputFormat;

public:
    VectorizedParquetRecordReader(const DB::Block & header, const DB::FormatSettings & format_settings);
    ~VectorizedParquetRecordReader() = default;

    void initialize(
        const DB::Block & header,
        const std::shared_ptr<arrow::io::RandomAccessFile> & arrow_file,
        const std::shared_ptr<ColumnIndexFilter> & column_index_filter,
        const std::shared_ptr<parquet::FileMetaData> & metadata = nullptr);
    DB::Chunk nextBatch();

    bool initialized() const { return parquetFileReader_ != nullptr; }

    void reset()
    {
        columnVectors_.clear();
        parquetFileReader_.reset();
    }
};


/// InputFormat

class VectorizedParquetBlockInputFormat final : public DB::IInputFormat
{
    std::atomic<int> is_stopped{0};
    DB::BlockMissingValues block_missing_values;
    VectorizedParquetRecordReader recordReader_;
    std::shared_ptr<ColumnIndexFilter> column_index_filter_;

protected:
    void onCancel() override { is_stopped = 1; }

public:
    VectorizedParquetBlockInputFormat(DB::ReadBuffer & in_, const DB::Block & header_, const DB::FormatSettings & format_settings_);
    void setColumnIndexFilter(const std::shared_ptr<ColumnIndexFilter> & column_index_filter)
    {
        column_index_filter_ = column_index_filter;
    }
    String getName() const override { return "VectorizedParquetBlockInputFormat"; }
    void resetParser() override;
    const DB::BlockMissingValues & getMissingValues() const override;

private:
    DB::Chunk read() override;
};
}
#endif
