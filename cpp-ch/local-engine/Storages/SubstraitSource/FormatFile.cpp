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
FormatFile::FormatFile(
    DB::ContextPtr context_,
    const substrait::ReadRel::LocalFiles::FileOrFiles & file_info_,
    const ReadBufferBuilderPtr & read_buffer_builder_)
    : context(context_), file_info(file_info_), read_buffer_builder(read_buffer_builder_)
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
            auto key = std::move(unescaped_key);
            partition_keys.push_back(key);
            partition_values[key] = std::move(unescaped_value);
        }
    }

    LOG_INFO(
        &Poco::Logger::get("FormatFile"),
        "Reading File path: {}, format: {}, range: {}, partition_index: {}, partition_values: {}",
        file_info.uri_file(),
        file_info.file_format_case(),
        std::to_string(file_info.start()) + "-" + std::to_string(file_info.start() + file_info.length()),
        file_info.partition_index(),
        GlutenStringUtils::dumpPartitionValues(partition_values));
}

FormatFilePtr FormatFileUtil::createFile(
    DB::ContextPtr context, ReadBufferBuilderPtr read_buffer_builder, const substrait::ReadRel::LocalFiles::FileOrFiles & file)
{
#if USE_PARQUET
    if (file.has_parquet())
    {
        auto config = ExecutorConfig::loadFromContext(context);
        return std::make_shared<ParquetFormatFile>(context, file, read_buffer_builder, config.use_local_format);
    }
#endif

#if USE_ORC
    if (file.has_orc())
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
