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
#include "ParquetOutputFormatFile.h"

#if USE_PARQUET

#    include <memory>
#    include <string>
#    include <utility>

#    include <Formats/FormatFactory.h>
#    include <Formats/FormatSettings.h>
#    include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#    include <Processors/Formats/Impl/CHColumnToArrowColumn.h>
#    include <Processors/Formats/Impl/ParquetBlockOutputFormat.h>
#    include <parquet/arrow/writer.h>
#    include <Common/Config.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
}

namespace local_engine
{
ParquetOutputFormatFile::ParquetOutputFormatFile(
    DB::ContextPtr context_,
    const std::string & file_uri_,
    WriteBufferBuilderPtr write_buffer_builder_,
    std::vector<std::string> & preferred_column_names_)
    : OutputFormatFile(context_, file_uri_, write_buffer_builder_, preferred_column_names_)
{
}

OutputFormatFile::OutputFormatPtr ParquetOutputFormatFile::createOutputFormat(const DB::Block & header)
{
    auto res = std::make_shared<OutputFormatFile::OutputFormat>();
    res->write_buffer = write_buffer_builder->build(file_uri);
    // TODO: align all spark parquet config with ch parquet config
    auto format_settings = DB::getFormatSettings(context);

    if (!preferred_column_names.empty())
    {
        //create a new header with the preferred column name
        DB::NamesAndTypesList names_types_list = header.getNamesAndTypesList();
        DB::ColumnsWithTypeAndName cols;
        size_t index = 0;
        for (const auto & name_type : header.getNamesAndTypesList())
        {
            if (name_type.name.starts_with("__bucket_value__"))
                continue;

            DB::ColumnWithTypeAndName col(name_type.type->createColumn(), name_type.type, preferred_column_names.at(index++));
            cols.emplace_back(col);
        }
        assert(preferred_column_names.size() == index);
        const DB::Block new_header(cols);
        auto output_format = std::make_shared<DB::ParquetBlockOutputFormat>(*(res->write_buffer), new_header, format_settings);
        res->output = output_format;
        return res;
    }
    else
    {
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "preferred_column_names is empty");
    }
}

}
#endif
