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
#include "FormatFile.h"
#include <memory>
#include <Core/Settings.h>
#include <IO/ReadBufferFromFile.h>
#include <Storages/SubstraitSource/JSONFormatFile.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <Common/GlutenConfig.h>
#include <Common/GlutenStringUtils.h>
#include <Common/logger_useful.h>


#if USE_PARQUET
#include <Storages/SubstraitSource/ParquetFormatFile.h>
#endif

#if USE_ORC
#include <Storages/SubstraitSource/ORCFormatFile.h>
#endif

#if USE_HIVE
#include <Storages/SubstraitSource/ExcelTextFormatFile.h>
#include <Storages/SubstraitSource/TextFormatFile.h>
#endif

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}
}
namespace local_engine
{
using namespace DB;
// Initialize the static variable outside the class definition
std::map<std::string, std::function<Field(const std::string &)>> FileMetaColumns::BASE_METADATA_EXTRACTORS
    = {{FILE_PATH, [](const std::string & metadata) { return metadata; }},
       {FILE_NAME, [](const std::string & metadata) { return metadata; }},
       {FILE_SIZE, [](const std::string & value) { return std::strtoll(value.c_str(), nullptr, 10); }},
       {FILE_BLOCK_START, [](const std::string & value) { return std::strtoll(value.c_str(), nullptr, 10); }},
       {FILE_BLOCK_LENGTH, [](const std::string & value) { return std::strtoll(value.c_str(), nullptr, 10); }},
       {FILE_MODIFICATION_TIME,
        [](const std::string & metadata)
        {
            DB::ReadBufferFromString in(metadata);
            DateTime64 time = 0;
            readDateTime64Text(time, 6, in, DateLUT::instance("UTC"));
            return DecimalField(time, 6);
        }}};

// Initialize the static variable outside the class definition
std::map<std::string, std::function<DB::Field(const substraitInputFile &)>> FileMetaColumns::INPUT_FUNCTION_EXTRACTORS
    = {{INPUT_FILE_NAME, [](const substraitInputFile & file) { return file.uri_file(); }},
       {INPUT_FILE_BLOCK_START, [](const substraitInputFile & file) { return file.start(); }},
       {INPUT_FILE_BLOCK_LENGTH, [](const substraitInputFile & file) { return file.length(); }}};

FileMetaColumns::FileMetaColumns(const substraitInputFile & file)
{
    for (const auto & column : file.metadata_columns())
    {
        if (!BASE_METADATA_EXTRACTORS.contains(column.key()))
            continue;

        assert(BASE_METADATA_EXTRACTORS.contains(column.key()));
        metadata_columns_map[column.key()] = BASE_METADATA_EXTRACTORS[column.key()](column.value());
    }

    for (const auto & inputExtractor : INPUT_FUNCTION_EXTRACTORS)
    {
        assert(!metadata_columns_map.contains(inputExtractor.first));
        metadata_columns_map[inputExtractor.first] = inputExtractor.second(file);
    }
}

DB::ColumnPtr FileMetaColumns::createMetaColumn(const String & columnName, const DB::DataTypePtr & type, size_t rows) const
{
    assert(metadata_columns_map.contains(columnName));
    const auto field = metadata_columns_map.at(columnName);

    if (INPUT_FILE_COLUMNS_SET.contains(columnName))
    {
        /// copied from InputFileNameParser::addInputFileColumnsToChunk()
        /// TODO: check whether using const column is correct or not.
        return type->createColumnConst(rows, field);
    }
    auto mutable_column = type->createColumn();
    mutable_column->insertMany(field, rows);
    return mutable_column;
}

FormatFile::FormatFile(DB::ContextPtr context_, const substraitInputFile & file_info_, const ReadBufferBuilderPtr & read_buffer_builder_)
    : context(context_), file_info(file_info_), read_buffer_builder(read_buffer_builder_), meta_columns(file_info_)
{
    if (file_info.partition_columns_size())
    {
        for (size_t i = 0; i < file_info.partition_columns_size(); ++i)
        {
            const auto & partition_column = file_info.partition_columns(i);

            std::string unescaped_key;
            std::string unescaped_value;
            Poco::URI::decode(partition_column.key(), unescaped_key);
            Poco::URI::decode(partition_column.value(), unescaped_value);

            partition_values[unescaped_key] = unescaped_value;

            std::string normalized_key = unescaped_key;
            boost::to_lower(normalized_key);
            normalized_partition_values[normalized_key] = unescaped_value;
        }
    }

    LOG_INFO(
        &Poco::Logger::get("FormatFile"),
        "Reading File path: {}, format: {}, range: {}, partition_index: {}, partition_values: {}",
        file_info.uri_file(),
        file_info.file_format_case(),
        std::to_string(file_info.start()) + "-" + std::to_string(file_info.start() + file_info.length()),
        file_info.partition_index(),
        GlutenStringUtils::mkString(partition_values));
}

FormatFilePtr FormatFileUtil::createFile(
    DB::ContextPtr context, ReadBufferBuilderPtr read_buffer_builder, const substrait::ReadRel::LocalFiles::FileOrFiles & file)
{
#if USE_PARQUET
    if (file.has_parquet() || (file.has_iceberg() && file.iceberg().has_parquet()))
    {
        auto config = ExecutorConfig::loadFromContext(context);
        return std::make_shared<ParquetFormatFile>(context, file, read_buffer_builder, config.use_local_format);
    }
#endif

#if USE_ORC
    if (file.has_orc() || (file.has_iceberg() && file.iceberg().has_orc()))
        return std::make_shared<ORCFormatFile>(context, file, read_buffer_builder);
#endif

#if USE_HIVE
    if (file.has_text())
    {
        if (ExcelTextFormatFile::useThis(context))
            return std::make_shared<ExcelTextFormatFile>(context, file, read_buffer_builder);
        else
            return std::make_shared<TextFormatFile>(context, file, read_buffer_builder);
    }
#endif

    if (file.has_json())
        return std::make_shared<JSONFormatFile>(context, file, read_buffer_builder);
    throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Format not supported:{}", file.DebugString());
}
}
