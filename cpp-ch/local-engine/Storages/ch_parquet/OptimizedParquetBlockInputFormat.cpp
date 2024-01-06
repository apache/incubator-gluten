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
#include "OptimizedParquetBlockInputFormat.h"
#include <boost/algorithm/string/case_conv.hpp>

#if USE_PARQUET
#include <DataTypes/NestedUtils.h>
#include <Formats/FormatFactory.h>
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <Storages/ch_parquet/OptimizedArrowColumnToCHColumn.h>
#include <Storages/ch_parquet/arrow/reader.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int CANNOT_READ_ALL_DATA;
}

#define THROW_ARROW_NOT_OK(status) \
    do \
    { \
        if (::arrow::Status _s = (status); !_s.ok()) \
            throw Exception::createRuntime(ErrorCodes::BAD_ARGUMENTS, _s.ToString()); \
    } while (false)

OptimizedParquetBlockInputFormat::OptimizedParquetBlockInputFormat(ReadBuffer & in_, Block header_, const FormatSettings & format_settings_)
    : IInputFormat(std::move(header_), &in_), format_settings(format_settings_)
{
}

Chunk OptimizedParquetBlockInputFormat::read()
{
    Chunk res;
    block_missing_values.clear();

    if (!file_reader)
        prepareReader();

    if (is_stopped)
        return {};

    if (row_group_current >= row_group_total)
        return res;

    std::shared_ptr<arrow::Table> table;
    arrow::Status read_status = file_reader->ReadRowGroup(row_group_current, column_indices, &table);
    if (!read_status.ok())
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Error while reading Parquet data: {}", read_status.ToString());

    if (format_settings.use_lowercase_column_name)
        table = *table->RenameColumns(column_names);

    ++row_group_current;

    arrow_column_to_ch_column->arrowTableToCHChunk(res, table);

    /// If defaults_for_omitted_fields is true, calculate the default values from default expression for omitted fields.
    /// Otherwise fill the missing columns with zero values of its type.
    if (format_settings.defaults_for_omitted_fields)
        for (size_t row_idx = 0; row_idx < res.getNumRows(); ++row_idx)
            for (const auto & column_idx : missing_columns)
                block_missing_values.setBit(column_idx, row_idx);
    return res;
}

void OptimizedParquetBlockInputFormat::resetParser()
{
    IInputFormat::resetParser();

    file_reader.reset();
    column_indices.clear();
    column_names.clear();
    row_group_current = 0;
    block_missing_values.clear();
}

const BlockMissingValues & OptimizedParquetBlockInputFormat::getMissingValues() const
{
    return block_missing_values;
}

static size_t countIndicesForType(std::shared_ptr<arrow::DataType> type)
{
    if (type->id() == arrow::Type::LIST)
        return countIndicesForType(static_cast<arrow::ListType *>(type.get())->value_type());

    if (type->id() == arrow::Type::STRUCT)
    {
        int indices = 0;
        auto * struct_type = static_cast<arrow::StructType *>(type.get());
        for (int i = 0; i != struct_type->num_fields(); ++i)
            indices += countIndicesForType(struct_type->field(i)->type());
        return indices;
    }

    if (type->id() == arrow::Type::MAP)
    {
        auto * map_type = static_cast<arrow::MapType *>(type.get());
        return countIndicesForType(map_type->key_type()) + countIndicesForType(map_type->item_type());
    }

    return 1;
}

static void getFileReaderAndSchema(
    ReadBuffer & in,
    std::unique_ptr<ch_parquet::arrow::FileReader> & file_reader,
    std::shared_ptr<arrow::Schema> & schema,
    const FormatSettings & format_settings,
    std::atomic<int> & is_stopped)
{
    auto arrow_file = asArrowFile(in, format_settings, is_stopped, "Parquet", PARQUET_MAGIC_BYTES);
    if (is_stopped)
        return;
    THROW_ARROW_NOT_OK(ch_parquet::arrow::OpenFile(std::move(arrow_file), arrow::default_memory_pool(), &file_reader));
    THROW_ARROW_NOT_OK(file_reader->GetSchema(&schema));

    if (format_settings.use_lowercase_column_name)
    {
        std::vector<std::shared_ptr<::arrow::Field>> fields;
        fields.reserve(schema->num_fields());
        for (int i = 0; i < schema->num_fields(); ++i)
        {
            const auto & field = schema->field(i);
            auto name = field->name();
            boost::to_lower(name);
            fields.push_back(field->WithName(name));
        }
        schema = arrow::schema(fields, schema->metadata());
    }
}

void OptimizedParquetBlockInputFormat::prepareReader()
{
    std::shared_ptr<arrow::Schema> schema;
    getFileReaderAndSchema(*in, file_reader, schema, format_settings, is_stopped);
    if (is_stopped)
        return;

    row_group_total = file_reader->num_row_groups();
    row_group_current = 0;

    arrow_column_to_ch_column = std::make_unique<OptimizedArrowColumnToCHColumn>(
        getPort().getHeader(), "Parquet", true, format_settings.parquet.allow_missing_columns);
    missing_columns = arrow_column_to_ch_column->getMissingColumns(*schema);

    std::unordered_set<String> nested_table_names;

    int index = 0;
    for (int i = 0; i < schema->num_fields(); ++i)
    {
        /// STRUCT type require the number of indexes equal to the number of
        /// nested elements, so we should recursively
        /// count the number of indices we need for this type.
        int indexes_count = countIndicesForType(schema->field(i)->type());
        const auto & name = schema->field(i)->name();
        if (getPort().getHeader().has(name) || nested_table_names.contains(name))
        {
            for (int j = 0; j != indexes_count; ++j)
            {
                column_indices.push_back(index + j);
                column_names.push_back(name);
            }
        }
        index += indexes_count;
    }
}

OptimizedParquetSchemaReader::OptimizedParquetSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_)
    : ISchemaReader(in_), format_settings(format_settings_)
{
}

NamesAndTypesList OptimizedParquetSchemaReader::readSchema()
{
    std::unique_ptr<ch_parquet::arrow::FileReader> file_reader;
    std::shared_ptr<arrow::Schema> schema;
    std::atomic<int> is_stopped = 0;
    getFileReaderAndSchema(in, file_reader, schema, format_settings, is_stopped);
    auto header = OptimizedArrowColumnToCHColumn::arrowSchemaToCHHeader(*schema, "Parquet");
    return header.getNamesAndTypesList();
}

/*
void registerInputFormatParquet(FormatFactory & factory)
{
    factory.registerInputFormat(
            "Parquet",
            [](ReadBuffer &buf,
                const Block &sample,
                const RowInputFormatParams &,
                const FormatSettings & settings)
            {
                return std::make_shared<OptimizedParquetBlockInputFormat>(buf, sample, settings);
            });
    factory.markFormatSupportsSubcolumns("Parquet");
    factory.markFormatSupportsSubsetOfColumns("Parquet");
}

void registerOptimizedParquetSchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader(
        "Parquet",
        [](ReadBuffer & buf, const FormatSettings & settings) { return std::make_shared<OptimizedParquetSchemaReader>(buf, settings); });

    factory.registerAdditionalInfoForSchemaCacheGetter(
        "Parquet",
        [](const FormatSettings & settings)
        { return fmt::format("schema_inference_make_columns_nullable={}", settings.schema_inference_make_columns_nullable); });
}
*/

}

#endif
