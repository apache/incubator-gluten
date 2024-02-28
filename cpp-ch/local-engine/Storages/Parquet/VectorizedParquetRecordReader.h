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
#include <Storages/Parquet/ArrowUtils.h>
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
class VectorizedParquetBlockInputFormat;

ColumnReadState buildAllRead(int64_t rg_count, const arrow::io::ReadRange & chunk_range);
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

using BuildRead = std::function<ColumnReadState(const arrow::io::ReadRange & col_range)>;
class PageIterator;

class ParquetFileReaderExt
{
    friend class PageIterator;
    std::shared_ptr<::arrow::io::RandomAccessFile> source_;
    int64_t source_size_;
    std::unique_ptr<parquet::ParquetFileReader> fileReader_;
    std::shared_ptr<ColumnIndexFilter> column_index_filter_;
    std::unordered_map<int32_t, std::unique_ptr<RowRanges>> row_group_row_ranges_;
    std::unordered_map<int32_t, std::unique_ptr<ColumnIndexStore>> row_group_column_index_stores_;

protected:
    std::unordered_set<int32_t> column_indices_;
    const RowRanges & getRowRanges(int32_t row_group);
    const ColumnIndexStore & getColumnIndexStore(int32_t row_group);

    bool canPruningPage(const int32_t row_group) const { return column_index_filter_ && RowGroupPageIndexReader(row_group) != nullptr; }
    std::unique_ptr<RowRanges> calculateRowRanges(const ColumnIndexStore & index_store, const size_t rowgroup_count) const
    {
        return std::make_unique<RowRanges>(column_index_filter_->calculateRowRanges(index_store, rowgroup_count));
    }
    std::unique_ptr<parquet::RowGroupMetaData> RowGroup(const int32_t row_group) const
    {
        const auto file_metadata = fileReader_->metadata();
        return file_metadata->RowGroup(row_group);
    }

    std::shared_ptr<parquet::RowGroupPageIndexReader> RowGroupPageIndexReader(const int32_t row_group) const
    {
        const auto pageIndex = fileReader_->GetPageIndexReader();
        return pageIndex == nullptr ? nullptr : pageIndex->RowGroup(row_group);
    }

    ColumnChunkPageRead
    readColumnChunkPageBase(const parquet::RowGroupMetaData & rg, int32_t column_index, const BuildRead & build_read) const;

public:
    ParquetFileReaderExt(
        const std::shared_ptr<::arrow::io::RandomAccessFile> & source,
        std::unique_ptr<parquet::ParquetFileReader> parquetFileReader,
        const std::shared_ptr<ColumnIndexFilter> & column_index_filter,
        const std::vector<int32_t> & column_indices);
};

class PageIterator final : public parquet::arrow::FileColumnIterator
{
    ParquetFileReaderExt * reader_ext_;

public:
    PageIterator(const int column_index, ParquetFileReaderExt * readerExt, const std::vector<int32_t> & row_groups)
        : FileColumnIterator(column_index, readerExt->fileReader_.get(), row_groups), reader_ext_(readerExt)
    {
    }

    ~PageIterator() override = default;

    std::optional<ColumnChunkPageRead> NextChunkWithRowRange()
    {
        while (!row_groups_.empty())
        {
            const int32_t row_group_index = row_groups_.front();
            const auto rg = reader_ext_->RowGroup(row_group_index);
            const auto rg_count = rg->num_rows();

            if (rg_count == 0)
            {
                row_groups_.pop_front();
                continue;
            }

            const RowRanges row_ranges = reader_ext_->canPruningPage(row_group_index) ? reader_ext_->getRowRanges(row_group_index)
                                                                                      : RowRanges::createSingle(rg_count);

            if (row_ranges.rowCount() == 0)
            {
                row_groups_.pop_front();
                continue;
            }

            const BuildRead readWithRowRange = [&](const arrow::io::ReadRange & col_range)
            {
                const ColumnIndexStore & column_index_store = reader_ext_->getColumnIndexStore(row_group_index);
                const ColumnIndex & index = *(column_index_store.find(descr()->name())->second);
                return buildRead(rg_count, col_range, index.GetOffsetIndex().page_locations(), row_ranges);
            };
            const BuildRead readAll = [&](const arrow::io::ReadRange & col_range) { return buildAllRead(rg_count, col_range); };

            const auto read = row_ranges.rowCount() == rg_count ? readAll : readWithRowRange;
            auto result = reader_ext_->readColumnChunkPageBase(*rg, column_index_, read);
            row_groups_.pop_front();
            return result;
        }
        return {};
    }
};

class VectorizedColumnReader
{
    std::shared_ptr<arrow::Field> arrowField_;
    PageIterator input_;
    std::shared_ptr<parquet::internal::RecordReader> record_reader_;
    std::unique_ptr<ParquetReadState> read_state_;

    void NextRowGroup();
    void SetPageReader(std::unique_ptr<parquet::PageReader> reader, const ReadSequence & read_sequence);

public:
    VectorizedColumnReader(
        const parquet::arrow::SchemaField & field, ParquetFileReaderExt * reader, const std::vector<int32_t> & row_groups);
    const std::string & col_name() const { return arrowField_->name(); }
    bool hasMoreRead() const { return read_state_ && read_state_->hasMoreRead(); }
    std::shared_ptr<arrow::ChunkedArray> readBatch(int64_t batch_size);
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

    bool initialize(
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
