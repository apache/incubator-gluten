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
    DB::ContextPtr context_, const std::string & file_uri_, WriteBufferBuilderPtr write_buffer_builder_)
    : OutputFormatFile(context_, file_uri_, write_buffer_builder_)
{
}

OutputFormatFile::OutputFormatPtr ParquetOutputFormatFile::createOutputFormat(const DB::Block & header)
{
    auto res = std::make_shared<OutputFormatFile::OutputFormat>();
    res->write_buffer = write_buffer_builder->build(file_uri);

    //TODO: align spark parquet config with ch parquet config
    auto format_settings = DB::getFormatSettings(context);
    auto output_format = std::make_shared<DB::ParquetBlockOutputFormat>(*(res->write_buffer), header, format_settings);

    res->output = output_format;
    return res;
}

}
#endif
