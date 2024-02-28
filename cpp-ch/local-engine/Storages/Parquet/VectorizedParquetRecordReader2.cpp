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
#include "VectorizedParquetRecordReader2.h"

#if USE_PARQUET
#include <optional>
#include <Processors/Chunk.h>
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <Processors/Formats/Impl/ArrowFieldIndexUtil.h>
#include <Storages/Parquet/ArrowUtils.h>
#include <boost/iterator/counting_iterator.hpp>
#include <parquet/column_reader.h>
#include <parquet/file_reader.h>


namespace local_engine2
{
VectorizedColumnReader::VectorizedColumnReader(
    const parquet::arrow::SchemaField & field, parquet::ParquetFileReader * reader, const std::vector<int32_t> & row_groups)
    : arrowField_(field.field)
    , input_(field.column_index, reader, row_groups)
    , record_reader_(parquet::internal::RecordReader::Make(
          input_.descr(),
          local_engine::ComputeLevelInfo(input_.descr()),
          local_engine::default_arrow_pool(),
          arrowField_->type()->id() == ::arrow::Type::DICTIONARY))
{
    NextRowGroup();
}

void VectorizedColumnReader::NextRowGroup()
{
    /// where IO happens!
    record_reader_->SetPageReader(input_.NextChunk());
}

void VectorizedColumnReader::readBatch(size_t batch_size, const ReadNextGroup & read_next_group) const
{
    while (batch_size > 0 && record_reader_->HasMoreData())
    {
        int64_t records_read = record_reader_->ReadRecords(batch_size);
        batch_size -= records_read;
        if (records_read == 0)
            read_next_group();
    }
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
    , manifest_()
{
}

void VectorizedParquetRecordReader::initialize(
    const DB::Block & header,
    const std::shared_ptr<arrow::io::RandomAccessFile> & arrow_file,
    const std::shared_ptr<parquet::FileMetaData> & metadata)
{
    parquetFileReader_ = parquet::ParquetFileReader::Open(arrow_file, parquet::default_reader_properties(), metadata);
    parquet::ArrowReaderProperties properties;
    const parquet::SchemaDescriptor * parquet_schema = parquetFileReader_->metadata()->schema();
    auto keyValueMetadata = parquetFileReader_->metadata()->key_value_metadata();
    THROW_ARROW_NOT_OK(parquet::arrow::SchemaManifest::Make(parquet_schema, keyValueMetadata, properties, &manifest_));
    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.reserve(manifest_.schema_fields.size());
    for (auto const & schema_field : manifest_.schema_fields)
        fields.emplace_back(schema_field.field);
    arrow::Schema schema(fields, keyValueMetadata);

    /// column pruning
    DB::ArrowFieldIndexUtil field_util(
        format_settings_.parquet.case_insensitive_column_matching, format_settings_.parquet.allow_missing_columns);
    std::vector<int32_t> column_indices = field_util.findRequiredIndices(header, schema);
    THROW_ARROW_NOT_OK_OR_ASSIGN(std::vector<int> field_indices, manifest_.GetFieldIndices(column_indices));

    /// row groups pruning
    std::vector<int32_t> row_groups(
        boost::counting_iterator<int32_t>(0), boost::counting_iterator<int32_t>(parquetFileReader_->metadata()->num_row_groups()));
    if (!format_settings_.parquet.skip_row_groups.empty())
        std::erase_if(row_groups, [&](int32_t i) { return format_settings_.parquet.skip_row_groups.contains(i); });

    assert(!row_groups.empty());
    columnVectors_.reserve(field_indices.size());
    for (auto const & fieldIndex : field_indices)
    {
        auto const & field = manifest_.schema_fields[fieldIndex];
        assert(field.column_index >= 0);
        columnVectors_.emplace_back(field, parquetFileReader_.get(), row_groups);
    }
}

DB::Chunk VectorizedParquetRecordReader::nextBatch()
{
    assert(initialized());
    const int64_t remainReads = format_settings_.parquet.max_block_size;
    for (auto & vectorized_column_reader : columnVectors_)
        vectorized_column_reader.prepareRead(remainReads);
    ::arrow::ChunkedArrayVector columns(columnVectors_.size());
    DB::ArrowColumnToCHColumn::NameToColumnPtr name_to_column_ptr;

    for (size_t i = 0; i < columnVectors_.size(); ++i)
    {
        auto & vectorized_column_reader = columnVectors_[i];
        vectorized_column_reader.readBatch(remainReads, [&]() { vectorized_column_reader.NextRowGroup(); });
        const std::shared_ptr<arrow::ChunkedArray> arrow_column = vectorized_column_reader.finishRead();
        std::string column_name = vectorized_column_reader.arrowField_->name();
        if (format_settings_.parquet.case_insensitive_column_matching)
            boost::to_lower(column_name);
        name_to_column_ptr[column_name] = arrow_column;
    }
    const size_t num_rows = name_to_column_ptr.begin()->second->length();
    if (num_rows > 0)
    {
        DB::Chunk result;
        arrowColumnToCHColumn_.arrowColumnsToCHChunk(result, name_to_column_ptr, num_rows, nullptr);
        return result;
    }
    return {};
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

    if (is_stopped)
        return {};

    if (!recordReader_.initialized())
    {
        auto arrow_file = DB::asArrowFile(*in, recordReader_.format_settings_, is_stopped, "Parquet", PARQUET_MAGIC_BYTES);
        if (is_stopped)
            return {};
        recordReader_.initialize(getPort().getHeader(), arrow_file);
    }
    return recordReader_.nextBatch();
}
}
#endif
