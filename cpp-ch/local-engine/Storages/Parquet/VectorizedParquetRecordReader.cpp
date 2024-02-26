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
#include "VectorizedParquetRecordReader.h"

#if USE_PARQUET
#include <numeric>
#include <optional>
#include <Processors/Chunk.h>
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <Processors/Formats/Impl/ArrowFieldIndexUtil.h>
#include <Storages/Parquet/ArrowUtils.h>
#include <arrow/io/memory.h>
#include <arrow/util/int_util_overflow.h>
#include <boost/iterator/counting_iterator.hpp>
#include <parquet/column_reader.h>
#include <parquet/file_reader.h>
#include <parquet/page_index.h>

namespace
{
bool extend(arrow::io::ReadRange & read_range, const int64_t offset, const int64_t length)
{
    if (read_range.offset + read_range.length == offset)
    {
        read_range.length += length;
        return true;
    }
    return false;
}
void emplaceReadRange(local_engine::ReadRanges & read_ranges, const int64_t offset, const int64_t length)
{
    if (read_ranges.empty() || !extend(read_ranges.back(), offset, length))
        read_ranges.emplace_back(arrow::io::ReadRange{offset, length});
}

/// Compute the section of the file that should be read for the given
/// row group and column chunk.
::arrow::io::ReadRange ComputeColumnChunkRange(
    const parquet::FileMetaData & file_metadata, const parquet::ColumnChunkMetaData & column_metadata, const int64_t source_size)
{
    // For PARQUET-816
    static constexpr int64_t kMaxDictHeaderSize = 100;

    int64_t col_start = column_metadata.data_page_offset();
    if (column_metadata.has_dictionary_page() && column_metadata.dictionary_page_offset() > 0
        && col_start > column_metadata.dictionary_page_offset())
        col_start = column_metadata.dictionary_page_offset();

    int64_t col_length = column_metadata.total_compressed_size();
    int64_t col_end;
    if (col_start < 0 || col_length < 0)
        throw parquet::ParquetException("Invalid column metadata (corrupt file?)");

    if (arrow::internal::AddWithOverflow(col_start, col_length, &col_end) || col_end > source_size)
        throw parquet::ParquetException("Invalid column metadata (corrupt file?)");

    // PARQUET-816 workaround for old files created by older parquet-mr
    const parquet::ApplicationVersion & version = file_metadata.writer_version();
    if (version.VersionLt(parquet::ApplicationVersion::PARQUET_816_FIXED_VERSION()))
    {
        // The Parquet MR writer had a bug in 1.2.8 and below where it didn't include the
        // dictionary page header size in total_compressed_size and total_uncompressed_size
        // (see IMPALA-694). We add padding to compensate.
        const int64_t bytes_remaining = source_size - col_end;
        const int64_t padding = std::min<int64_t>(kMaxDictHeaderSize, bytes_remaining);
        col_length += padding;
    }
    return {col_start, col_length};
}
}

namespace local_engine
{
VectorizedColumnReader::VectorizedColumnReader(
    const int32_t column_index, const std::shared_ptr<arrow::Field> & field, const parquet::ColumnDescriptor * descr)
    : column_index_(column_index)
    , field_(field)
    , descr_(descr)
    , record_reader_(parquet::internal::RecordReader::Make(
          descr_, ComputeLevelInfo(descr_), default_arrow_pool(), field_->type()->id() == ::arrow::Type::DICTIONARY))
{
}
void VectorizedColumnReader::SetPageReader(std::unique_ptr<parquet::PageReader> reader, const ReadSequence & read_sequence)
{
    if (read_state_)
    {
        read_state_->hasLastSkip().and_then(
            [&](const int64_t skip) -> std::optional<int64_t>
            {
                assert(skip < 0);
                record_reader_->SkipRecords(-skip);
                return std::nullopt;
            });
    }
    record_reader_->SetPageReader(std::move(reader));
    read_state_ = std::make_unique<ParquetReadState>(read_sequence);
}

void VectorizedColumnReader::prepareRead(const int64_t batch_size) const
{
    assert(read_state_);
    record_reader_->Reset();
    record_reader_->Reserve(batch_size);
}

int64_t VectorizedColumnReader::readBatch(const int64_t batch_size) const
{
    size_t read = 0;
    while (read_state_->hasMoreRead() && batch_size > read)
    {
        const int64_t readNumber = read_state_->currentRead();
        if (readNumber < 0)
        {
            const int64_t records_skipped = record_reader_->SkipRecords(-readNumber);
            assert(records_skipped == -readNumber);
            read_state_->skip(records_skipped);
        }
        else
        {
            const int64_t readBatch = std::min(batch_size, readNumber);
            const int64_t records_read = record_reader_->ReadRecords(readBatch);
            assert(records_read == readBatch);
            read += records_read;
            read_state_->read(records_read);
        }
    }
    return read;
}

std::shared_ptr<arrow::ChunkedArray> VectorizedColumnReader::finishRead() const
{
    std::shared_ptr<arrow::ChunkedArray> result;
    THROW_ARROW_NOT_OK(parquet::arrow::TransferColumnData(record_reader_.get(), field_, descr_, default_arrow_pool(), &result));
    return result;
}

VectorizedParquetRecordReader::VectorizedParquetRecordReader(const DB::Block & header, const DB::FormatSettings & format_settings)
    : format_settings_(format_settings)
    , arrowColumnToCHColumn_(
          header,
          "Parquet",
          format_settings.parquet.allow_missing_columns,
          format_settings.null_as_default,
          format_settings.date_time_overflow_behavior,
          format_settings.parquet.case_insensitive_column_matching)
{
}

void VectorizedParquetRecordReader::initialize(
    const DB::Block & header,
    const std::shared_ptr<arrow::io::RandomAccessFile> & arrow_file,
    const std::shared_ptr<ColumnIndexFilter> & column_index_filter,
    const std::shared_ptr<parquet::FileMetaData> & metadata)
{
    auto file_reader = parquet::ParquetFileReader::Open(arrow_file, parquet::default_reader_properties(), metadata);
    const parquet::ArrowReaderProperties properties;
    const parquet::FileMetaData & file_metadata = *(file_reader->metadata());
    const parquet::SchemaDescriptor * parquet_schema = file_metadata.schema();
    const auto keyValueMetadata = file_metadata.key_value_metadata();
    parquet::arrow::SchemaManifest manifest;
    THROW_ARROW_NOT_OK(parquet::arrow::SchemaManifest::Make(parquet_schema, keyValueMetadata, properties, &manifest));
    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.reserve(manifest.schema_fields.size());
    for (auto const & schema_field : manifest.schema_fields)
        fields.emplace_back(schema_field.field);
    const arrow::Schema schema(fields, keyValueMetadata);

    /// column pruning
    DB::ArrowFieldIndexUtil field_util(
        format_settings_.parquet.case_insensitive_column_matching, format_settings_.parquet.allow_missing_columns);
    const std::vector<int32_t> column_indices = field_util.findRequiredIndices(header, schema);
    THROW_ARROW_NOT_OK_OR_ASSIGN(std::vector<int> field_indices, manifest.GetFieldIndices(column_indices));

    /// row groups pruning
    std::vector<int32_t> row_groups(
        boost::counting_iterator<int32_t>(0), boost::counting_iterator<int32_t>(file_metadata.num_row_groups()));
    if (!format_settings_.parquet.skip_row_groups.empty())
    {
        row_groups.erase(
            std::ranges::remove_if(row_groups, [&](const int32_t i) { return format_settings_.parquet.skip_row_groups.contains(i); })
                .begin(),
            row_groups.end());
    }

    assert(!row_groups.empty());
    parquetFileReader_
        = std::make_unique<ParquetFileReaderExt>(arrow_file, std::move(file_reader), column_index_filter, row_groups, field_indices);
    auto pageStores = parquetFileReader_->readFilteredRowGroups();
    assert(pageStores);

    columnVectors_.reserve(field_indices.size());

    for (auto const & column_index : field_indices)
    {
        auto const & field = manifest.schema_fields[column_index];
        assert(field.column_index >= 0);
        assert(column_index == field.column_index);
        columnVectors_.emplace_back(column_index, field.field, parquet_schema->Column(column_index));
        auto & [page_reader, page_read_infos] = pageStores->find(column_index)->second;
        assert(page_reader);
        columnVectors_.back().SetPageReader(std::move(page_reader), page_read_infos);
    }
}

DB::Chunk VectorizedParquetRecordReader::nextBatch()
{
    assert(initialized());

    int64_t remainReads = format_settings_.parquet.max_block_size;
    for (auto & vectorized_column_reader : columnVectors_)
        vectorized_column_reader.prepareRead(remainReads);
    std::vector<int64_t> read_numbers(columnVectors_.size());

    do
    {
        for (size_t i = 0; i < columnVectors_.size(); ++i)
            read_numbers[i] += columnVectors_[i].readBatch(remainReads);

        const int64_t readReords = read_numbers.back();
#ifndef NDEBUG
        for (const auto & read_number : read_numbers)
            assert(read_number == readReords);
#endif

        remainReads -= readReords;
        if (remainReads == 0)
            break;

        const auto pageStores = parquetFileReader_->readFilteredRowGroups();
        if (!pageStores)
            break;

        for (auto & vectorized_column_reader : columnVectors_)
        {
            auto & [page_reader, page_read_infos] = pageStores->find(vectorized_column_reader.column_index())->second;
            assert(page_reader);
            vectorized_column_reader.SetPageReader(std::move(page_reader), page_read_infos);
        }
    } while (true);

    if (remainReads == format_settings_.parquet.max_block_size) //we didn't read any thing.
        return {};

    ::arrow::ChunkedArrayVector columns(columnVectors_.size());
    DB::ArrowColumnToCHColumn::NameToColumnPtr name_to_column_ptr;
    for (const auto & vectorized_column_reader : columnVectors_)
    {
        const std::shared_ptr<arrow::ChunkedArray> arrow_column = vectorized_column_reader.finishRead();

        std::string column_name = vectorized_column_reader.field_->name();
        if (format_settings_.parquet.case_insensitive_column_matching)
            boost::to_lower(column_name);
        name_to_column_ptr[column_name] = arrow_column;
    }

    const size_t num_rows = name_to_column_ptr.begin()->second->length();
    DB::Chunk result;
    arrowColumnToCHColumn_.arrowColumnsToCHChunk(result, name_to_column_ptr, num_rows, nullptr);
    return result;
}

ParquetFileReaderExtBase::ParquetFileReaderExtBase(
    const std::shared_ptr<arrow::io::RandomAccessFile> & source,
    std::unique_ptr<parquet::ParquetFileReader> parquetFileReader,
    const std::shared_ptr<ColumnIndexFilter> & column_index_filter,
    const std::vector<int32_t> & column_indices)
    : source_(source)
    , fileReader_(std::move(parquetFileReader))
    , column_index_filter_(column_index_filter)
    , column_indices_(column_indices.begin(), column_indices.end())
{
    THROW_ARROW_NOT_OK_OR_ASSIGN(const int64_t source_size, source_->GetSize());
    source_size_ = source_size;
}

ColumnChunkPageRead ParquetFileReaderExtBase::readColumnChunkPageBase(
    const parquet::RowGroupMetaData & rg, const int32_t column_index, const BuildRead & build_read) const
{
    const auto file_metadata = fileReader_->metadata();

    // Prior to Arrow 3.0.0, is_compressed was always set to false in column headers,
    // even if compression was used. See ARROW-17100.
    const bool always_compressed
        = file_metadata->writer_version().VersionLt(parquet::ApplicationVersion::PARQUET_CPP_10353_FIXED_VERSION());
    const auto column_metadata = rg.ColumnChunk(column_index);
    const auto col_range = ComputeColumnChunkRange(*file_metadata, *column_metadata, source_size_);
    const parquet::ReaderProperties properties;
    auto [read_ranges, read_sequence] = build_read(column_index, col_range);
    const auto input_stream = getStream(*source_, read_ranges);
    return std::make_pair(
        parquet::PageReader::Open(
            input_stream, column_metadata->num_values(), column_metadata->compression(), properties, always_compressed),
        read_sequence);
}

const RowRanges & ParquetFileReaderExtBase::getRowRanges(const int32_t row_group)
{
    if (!row_group_row_ranges_.contains(row_group))
    {
        const auto rowGroup = RowGroup(row_group);
        const ColumnIndexStore & column_index_store = getColumnIndexStore(row_group);
        row_group_row_ranges_[row_group] = calculateRowRanges(column_index_store, rowGroup->num_rows());
    }
    return *(row_group_row_ranges_[row_group]);
}

const ColumnIndexStore & ParquetFileReaderExtBase::getColumnIndexStore(const int32_t row_group)
{
    if (!row_group_column_index_stores_.contains(row_group))
    {
        const auto rowGroup = RowGroup(row_group);
        const auto rowGroupIndex = RowGroupPageIndexReader(row_group);

        auto result = std::make_unique<ColumnIndexStore>();
        ColumnIndexStore & column_index_store = *result;
        column_index_store.reserve(column_indices_.size());

        for (auto const column_index : column_indices_)
        {
            const auto * col_desc = rowGroup->schema()->Column(column_index);
            const auto col_index = rowGroupIndex->GetColumnIndex(column_index);
            const auto offset_index = rowGroupIndex->GetOffsetIndex(column_index);
            column_index_store[col_desc->name()] = ColumnIndex::Make(col_desc, col_index, offset_index);
        }
        row_group_column_index_stores_[row_group] = std::move(result);
    }
    return *(row_group_column_index_stores_[row_group]);
}

ColumnChunkPageReadStorePtr
ParquetFileReaderExt::readRowGroupsBase(const parquet::RowGroupMetaData & rg, const BuildRead & build_read) const
{
    const int columns = rg.num_columns();
    auto result = std::make_unique<ColumnChunkPageReadStore>();
    result->reserve(column_indices_.size());
    for (int column_index = 0; column_index < columns; ++column_index)
    {
        if (!column_indices_.contains(column_index))
            continue;
        result->emplace(column_index, readColumnChunkPageBase(rg, column_index, build_read));
    }
    return result;
}

ColumnChunkPageReadStorePtr ParquetFileReaderExt::readFilteredRowGroups(
    const parquet::RowGroupMetaData & rg, const RowRanges & row_ranges, const ColumnIndexStore & column_index_store) const
{
    return readRowGroupsBase(
        rg,
        [&](const int32_t column_index, const arrow::io::ReadRange & col_range)
        {
            const auto * col_desc = rg.schema()->Column(column_index);
            const ColumnIndex & index = *(column_index_store.find(col_desc->name())->second);
            return buildRead(rg.num_rows(), col_range, index.GetOffsetIndex().page_locations(), row_ranges);
        });
}

ColumnChunkPageReadStorePtr ParquetFileReaderExt::readRowGroups(const parquet::RowGroupMetaData & rg) const
{
    return readRowGroupsBase(rg, [&](int32_t, const arrow::io::ReadRange & col_range) { return buildAllRead(rg.num_rows(), col_range); });
}

ParquetFileReaderExt::ParquetFileReaderExt(
    const std::shared_ptr<arrow::io::RandomAccessFile> & source,
    std::unique_ptr<parquet::ParquetFileReader> parquetFileReader,
    const std::shared_ptr<ColumnIndexFilter> & column_index_filter,
    const std::vector<int32_t> & row_groups,
    const std::vector<int32_t> & column_indices)
    : ParquetFileReaderExtBase(source, std::move(parquetFileReader), column_index_filter, column_indices)
    , row_groups_(row_groups.begin(), row_groups.end())
{
}

ColumnChunkPageReadStorePtr ParquetFileReaderExt::readFilteredRowGroups()
{
    while (hasMoreRead())
    {
        const int32_t row_group = row_groups_.front();
        const auto rowGroup = RowGroup(row_group);

        if (rowGroup->num_rows() == 0) // Skip Empty RowGroup
        {
            advanceRowGroup();
            continue;
        }

        ColumnChunkPageReadStorePtr result;
        if (canPruningPage(row_group))
        {
            const RowRanges & row_ranges = getRowRanges(row_group);
            if (row_ranges.rowCount() == 0) // There are no matching rows in this row group -> skipping it
            {
                advanceRowGroup();
                continue;
            }
            result = readFilteredRowGroups(*rowGroup, row_ranges, getColumnIndexStore(row_group));
        }
        else
        {
            result = readRowGroups(*rowGroup);
        }
        advanceRowGroup();
        return result;
    }
    return nullptr;
}

/// input format
VectorizedParquetBlockInputFormat::VectorizedParquetBlockInputFormat(
    DB::ReadBuffer & in_, const DB::Block & header_, const DB::FormatSettings & format_settings)
    : DB::IInputFormat(header_, &in_), recordReader_(getPort().getHeader(), format_settings)
{
}

void VectorizedParquetBlockInputFormat::resetParser()
{
    IInputFormat::resetParser();
    recordReader_.reset();
    block_missing_values.clear();
}

const DB::BlockMissingValues & VectorizedParquetBlockInputFormat::getMissingValues() const
{
    return block_missing_values;
}

DB::Chunk VectorizedParquetBlockInputFormat::read()
{
    block_missing_values.clear();

    if (is_stopped != 0)
        return {};

    if (!recordReader_.initialized())
    {
        const auto arrow_file = DB::asArrowFile(*in, recordReader_.format_settings_, is_stopped, "Parquet", PARQUET_MAGIC_BYTES);
        if (is_stopped != 0)
            return {};
        recordReader_.initialize(getPort().getHeader(), arrow_file, column_index_filter_);
    }
    return recordReader_.nextBatch();
}
ColumnReadState buildAllRead(const int64_t rg_count, const arrow::io::ReadRange & chunk_range)
{
    return std::make_pair(ReadRanges{chunk_range}, ReadSequence{rg_count});
}

ColumnReadState buildRead(
    const int64_t rg_count,
    const arrow::io::ReadRange & chunk_range,
    const std::vector<parquet::PageLocation> & page_locations,
    const RowRanges & row_ranges)
{
    if (rg_count == row_ranges.rowCount())
        return buildAllRead(rg_count, chunk_range);

    auto skip = [](ReadSequence & read_sequence, const int64_t number) -> void
    {
        assert(number > 0);
        if (read_sequence.empty() || read_sequence.back() > 0)
            read_sequence.push_back(-number);
        else
            read_sequence.back() -= number;
    };

    auto read = [](ReadSequence & read_sequence, const int64_t number) -> void
    {
        assert(number > 0);
        if (read_sequence.empty() || read_sequence.back() < 0)
            read_sequence.push_back(number);
        else
            read_sequence.back() += number;
    };

    ColumnReadState result;
    const RowRangesBuilder rg_builder{rg_count, page_locations};
    ReadRanges & read_ranges = result.first;

    // Add a range for dictionary page if required
    if (chunk_range.offset < page_locations[0].offset)
        emplaceReadRange(read_ranges, chunk_range.offset, page_locations[0].offset - chunk_range.offset);

    std::vector<Range> page_row_ranges;
    const size_t pageSize = page_locations.size();
    for (size_t pageIndex = 0; pageIndex < pageSize; ++pageIndex)
    {
        size_t from = rg_builder.firstRowIndex(pageIndex);
        size_t to = rg_builder.lastRowIndex(pageIndex);
        if (row_ranges.isOverlapping(from, to))
        {
            emplaceReadRange(read_ranges, page_locations[pageIndex].offset, page_locations[pageIndex].compressed_page_size);
            page_row_ranges.push_back({from, to});
        }
    }

    result.second.resize(1);
    std::vector<int64_t> & read_sequence = result.second;

    auto row_range_begin = row_ranges.getRanges().begin();
    const auto row_range_end = row_ranges.getRanges().end();
    size_t rowIndex = row_range_begin->from;

    for (size_t i = 0; i < page_row_ranges.size() && row_range_begin != row_range_end; ++i)
    {
        const size_t lastRowIndexInPage = page_row_ranges[i].to;
        size_t readRowIndexInPage = page_row_ranges[i].from;

        /// [readRowIndexInPage ,rowIndex-1] - [rowIndex, rowIndex+readNumber-1] - [rowIndex+readNumber, lastRowIndexInPage]
        if (rowIndex <= lastRowIndexInPage)
        {
            assert(rowIndex >= readRowIndexInPage);
            do
            {
                if (rowIndex > readRowIndexInPage)
                    skip(read_sequence, rowIndex - readRowIndexInPage);
                const size_t readNumber = std::min(row_range_begin->to, lastRowIndexInPage) - rowIndex + 1;
                read(read_sequence, readNumber);
                rowIndex += readNumber;
                readRowIndexInPage = rowIndex;

                /// we already read cuurent page, so we need to read next page.
                if (row_range_begin->to > lastRowIndexInPage)
                {
                    assert(readRowIndexInPage > lastRowIndexInPage);
                    break;
                }

                /// we already read current range so we need read next range
                ++row_range_begin;
                if (row_range_begin != row_range_end)
                    rowIndex = row_range_begin->from;
            } while (row_range_begin != row_range_end && rowIndex <= lastRowIndexInPage);

            /// skip read remain records in current page.
            if (readRowIndexInPage <= lastRowIndexInPage)
                skip(read_sequence, lastRowIndexInPage - readRowIndexInPage + 1);
        }
        assert(!read_sequence.empty());
    }
    return result;
}

std::shared_ptr<parquet::ArrowInputStream> getStream(arrow::io::RandomAccessFile & reader, const ReadRanges & ranges)
{
    const int64_t nbytes = std::accumulate(
        ranges.begin(), ranges.end(), 0, [](const int64_t total, const arrow::io::ReadRange & range) { return total + range.length; });
    THROW_ARROW_NOT_OK_OR_ASSIGN(std::shared_ptr<arrow::Buffer> buffer, arrow::AllocateResizableBuffer(nbytes));

    int64_t readPos = 0;
    std::ranges::for_each(
        ranges,
        [&](const arrow::io::ReadRange & range)
        {
            const int64_t offset = range.offset;
            const int64_t length = range.length;
            THROW_ARROW_NOT_OK_OR_ASSIGN(const int64_t bytes_read, reader.ReadAt(offset, length, buffer->mutable_data() + readPos));
            assert(bytes_read == length);
            readPos += bytes_read;
        });
    assert(nbytes == readPos);

    return std::make_shared<::arrow::io::BufferReader>(buffer);
}

}
#endif
