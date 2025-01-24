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

#include "DataTypes/DataTypeNullable.h"


#include <config.h>

#if USE_PARQUET

#include <Formats/FormatSettings.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Storages/Parquet/ColumnIndexFilter.h>
#include <absl/container/flat_hash_map.h>
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
using ColumnReadState = std::pair<ReadRanges, ReadSequence>;
using ColumnChunkPageRead = std::pair<std::unique_ptr<parquet::PageReader>, ReadSequence>;

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

/// UT
class IRowRangesProvider
{
public:
    virtual ~IRowRangesProvider() = default;
    virtual std::optional<RowRanges> getRowRanges(Int32 row_group_index) = 0;
};

class ParquetFileReaderExt : public IRowRangesProvider
{
    using RowRangesMap = absl::flat_hash_map<Int32, std::unique_ptr<RowRanges>>;
    using ColumnIndexStoreMap = absl::flat_hash_map<Int32, std::unique_ptr<ColumnIndexStore>>;

    /// Members
    std::shared_ptr<::arrow::io::RandomAccessFile> source_;
    int64_t source_size_;
    std::unique_ptr<parquet::ParquetFileReader> file_reader_;
    ColumnIndexFilterPtr column_index_filter_;
    RowRangesMap row_group_row_ranges_;
    ColumnIndexStoreMap row_group_column_index_stores_;
    const DB::FormatSettings & format_settings_;
    std::unordered_set<Int32> column_indices_;

    /// Methods
    const RowRanges & internalGetRowRanges(Int32 row_group);
    const ColumnIndexStore & getColumnIndexStore(Int32 row_group);

    bool canPruningPage(const Int32 row_group) const { return column_index_filter_ && rowGroupPageIndexReader(row_group) != nullptr; }
    std::unique_ptr<RowRanges> calculateRowRanges(const ColumnIndexStore & index_store, const size_t rowgroup_count) const
    {
        return std::make_unique<RowRanges>(column_index_filter_->calculateRowRanges(index_store, rowgroup_count));
    }
    std::unique_ptr<parquet::RowGroupMetaData> rowGroup(const Int32 row_group) const
    {
        const auto file_metadata = file_reader_->metadata();
        return file_metadata->RowGroup(row_group);
    }

    std::shared_ptr<parquet::RowGroupPageIndexReader> rowGroupPageIndexReader(const Int32 row_group) const
    {
        const auto pageIndex = file_reader_->GetPageIndexReader();
        return pageIndex == nullptr ? nullptr : pageIndex->RowGroup(row_group);
    }

    ColumnChunkPageRead
    readColumnChunkPageBase(const parquet::RowGroupMetaData & rg, Int32 column_index, const BuildRead & build_read) const;

public:
    ParquetFileReaderExt(
        const std::shared_ptr<::arrow::io::RandomAccessFile> & source,
        std::unique_ptr<parquet::ParquetFileReader> parquetFileReader,
        const ColumnIndexFilterPtr & column_index_filter,
        const std::vector<Int32> & column_indices,
        const DB::FormatSettings & format_settings);

    std::optional<ColumnChunkPageRead> nextRowGroup(int32_t row_group_index, int32_t column_index, const std::string & column_name);

    parquet::ParquetFileReader * fileReader() const { return file_reader_.get(); }

    std::optional<RowRanges> getRowRanges(Int32 row_group_index) override
    {
        const auto rg = rowGroup(row_group_index);
        const auto rg_count = rg->num_rows();

        if (rg_count == 0 || (canPruningPage(row_group_index) && internalGetRowRanges(row_group_index).rowCount() == 0))
            return std::nullopt;

        return canPruningPage(row_group_index) ? internalGetRowRanges(row_group_index) : RowRanges::createSingle(rg_count);
    }
};

class PageIterator final : public parquet::arrow::FileColumnIterator
{
    ParquetFileReaderExt * reader_ext_;

public:
    PageIterator(const int column_index, ParquetFileReaderExt * reader_ext, const std::vector<Int32> & row_groups)
        : FileColumnIterator(column_index, reader_ext->fileReader(), row_groups), reader_ext_(reader_ext)
    {
    }

    ~PageIterator() override = default;

    std::optional<ColumnChunkPageRead> nextRowGroup();
};

class RowIndexGenerator
{
    RowRanges row_ranges_;
    size_t start_rowRange_ = 0;

public:
    RowIndexGenerator(const RowRanges & row_ranges, UInt64 startingRowIdx) : row_ranges_(row_ranges)
    {
        assert(!row_ranges_.getRanges().empty());
        for (auto & range : row_ranges_.getRanges())
        {
            range.from += startingRowIdx;
            range.to += startingRowIdx;
        }
    }

    size_t populateRowIndices(Int64 * row_indices, size_t batchSize)
    {
        size_t count = 0;
        for (; start_rowRange_ < row_ranges_.getRanges().size(); ++start_rowRange_)
        {
            auto & range = row_ranges_.getRange(start_rowRange_);
            for (size_t j = range.from; j <= range.to; ++j)
            {
                *row_indices++ = j;
                if (++count >= batchSize)
                {
                    range.from = j + 1; // Advance range.from
                    return count;
                }
            }
        }
        return count;
    }
};

namespace ParquetVirtualMeta
{
inline constexpr auto TMP_ROWINDEX = "_tmp_metadata_row_index";
inline bool hasMetaColumns(const DB::Block & header)
{
    return header.findByName(TMP_ROWINDEX) != nullptr;
}
inline DB::DataTypePtr getMetaColumnType(const DB::Block & header)
{
    return header.findByName(TMP_ROWINDEX)->type;
}
inline DB::Block removeMetaColumns(const DB::Block & header)
{
    DB::Block new_header;
    for (const auto & col : header)
        if (col.name != TMP_ROWINDEX)
            new_header.insert(col);
    return new_header;
}
}

class VirtualColumnRowIndexReader
{
    IRowRangesProvider * row_ranges_provider_;
    std::deque<Int32> row_groups_;
    std::vector<UInt64> row_group_ordinal_to_row_idx_idx_;
    std::optional<RowIndexGenerator> row_index_generator_;
    DB::DataTypePtr column_type_;

    std::optional<RowIndexGenerator> nextRowGroup()
    {
        while (!row_groups_.empty())
        {
            const Int32 row_group_index = row_groups_.front();
            auto result = row_ranges_provider_->getRowRanges(row_group_index);
            row_groups_.pop_front();
            if (result)
                return RowIndexGenerator{result.value(), row_group_ordinal_to_row_idx_idx_[row_group_index]};
        }
        return std::nullopt;
    }

public:
    VirtualColumnRowIndexReader(
        IRowRangesProvider * row_ranges_provider,
        const std::vector<Int32> & row_groups,
        const std::vector<UInt64> & row_group_ordinal_to_row_idx_idx,
        const DB::DataTypePtr & column_type)
        : row_ranges_provider_(row_ranges_provider)
        , row_groups_(row_groups.begin(), row_groups.end())
        , row_group_ordinal_to_row_idx_idx_(row_group_ordinal_to_row_idx_idx)
        , row_index_generator_(nextRowGroup())
        , column_type_(column_type)
    {
    }

    DB::ColumnPtr readBatch(const int64_t batch_size)
    {
        if (column_type_->isNullable())
        {
            auto internal_type = typeid_cast<const DB::DataTypeNullable &>(*column_type_).getNestedType();
            auto nested_column = readBatchNonNullable(internal_type, batch_size);
            auto nullmap_column = DB::ColumnUInt8::create(nested_column->size(), 0);
            return DB::ColumnNullable::create(nested_column, std::move(nullmap_column));
        }
        return readBatchNonNullable(column_type_, batch_size);
    }

    DB::ColumnPtr readBatchNonNullable(const DB::DataTypePtr & notNullType, const int64_t batch_size)
    {
        assert(DB::WhichDataType(notNullType).isInt64());
        auto column = DB::ColumnInt64::create(batch_size);
        DB::ColumnInt64::Container & vec = column->getData();
        int64_t readCount = 0;
        int64_t remaining = batch_size;
        while (remaining > 0 && row_index_generator_)
        {
            Int64 * pos = vec.data() + readCount;
            readCount += row_index_generator_->populateRowIndices(pos, remaining);
            remaining = batch_size - readCount;
            if (remaining > 0)
                row_index_generator_ = nextRowGroup();
        }

        if (remaining) // we know that we have read all the rows, but we can't fill the container
            vec.resize(readCount);
        assert(readCount + remaining == batch_size);
        assert(readCount == column->size());
        return column;
    }
};

class VectorizedColumnReader
{
    std::shared_ptr<arrow::Field> arrow_field_;
    PageIterator input_;
    std::shared_ptr<parquet::internal::RecordReader> record_reader_;
    std::unique_ptr<ParquetReadState> read_state_;

    void nextRowGroup();
    void setPageReader(std::unique_ptr<parquet::PageReader> reader, const ReadSequence & read_sequence);

public:
    VectorizedColumnReader(const parquet::arrow::SchemaField & field, ParquetFileReaderExt * reader, const std::vector<Int32> & row_groups);
    const std::string & columnName() const { return arrow_field_->name(); }
    std::shared_ptr<arrow::Field> arrowField() const { return arrow_field_; }
    bool hasMoreRead() const { return read_state_ && read_state_->hasMoreRead(); }
    std::shared_ptr<arrow::ChunkedArray> readBatch(int64_t batch_size);
};

class VectorizedParquetRecordReader
{
    const DB::Block original_header_;
    const DB::Block parquet_header_;
    const DB::FormatSettings format_settings_;
    DB::ArrowColumnToCHColumn arrow_column_to_ch_column_;

    std::unique_ptr<ParquetFileReaderExt> file_reader_;

    // parquet::arrow::SchemaManifest manifest_;
    /// columns to read from Parquet file.
    std::vector<VectorizedColumnReader> column_readers_;
    std::optional<VirtualColumnRowIndexReader> row_index_reader_ = std::nullopt;

    friend class VectorizedParquetBlockInputFormat;

    static parquet::arrow::SchemaManifest createSchemaManifest(const parquet::FileMetaData & metadata);
    std::vector<Int32> pruneColumn(const DB::Block & header, const parquet::FileMetaData & metadata) const;
    std::vector<Int32> pruneRowGroups(const parquet::FileMetaData & file_metadata) const;


    static std::vector<UInt64> calculateRowGroupOffsets(const parquet::FileMetaData & file_metadata)
    {
        UInt64 rowIdxSum = 0;
        std::vector<UInt64> rowGroupOrdinalToRowIdx;
        for (size_t i = 0; i < file_metadata.num_row_groups(); i++)
        {
            rowGroupOrdinalToRowIdx.push_back(rowIdxSum);
            rowIdxSum += file_metadata.RowGroup(i)->num_rows();
        }
        return rowGroupOrdinalToRowIdx;
    }

public:
    VectorizedParquetRecordReader(const DB::Block & header, const DB::FormatSettings & format_settings);
    ~VectorizedParquetRecordReader() = default;

    bool initialize(
        const std::shared_ptr<arrow::io::RandomAccessFile> & arrow_file,
        const ColumnIndexFilterPtr & column_index_filter,
        const std::shared_ptr<parquet::FileMetaData> & metadata = nullptr);
    DB::Chunk nextBatch();

    bool initialized() const { return file_reader_ != nullptr; }

    void reset()
    {
        column_readers_.clear();
        file_reader_.reset();
    }
};

/// InputFormat

class VectorizedParquetBlockInputFormat final : public DB::IInputFormat
{
    std::atomic<int> is_stopped{0};
    VectorizedParquetRecordReader record_reader_;
    ColumnIndexFilterPtr column_index_filter_;

protected:
    void onCancel() noexcept override { is_stopped = 1; }

public:
    VectorizedParquetBlockInputFormat(
        DB::ReadBuffer & in_,
        const DB::Block & header_,
        const ColumnIndexFilterPtr & column_index_filter,
        const DB::FormatSettings & format_settings);
    String getName() const override { return "VectorizedParquetBlockInputFormat"; }
    void resetParser() override;

private:
    DB::Chunk read() override;
};
}
#endif
