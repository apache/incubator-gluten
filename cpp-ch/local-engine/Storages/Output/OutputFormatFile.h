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

#include <memory>
#include <vector>

#include <Core/Block.h>
#include <IO/WriteBuffer.h>
#include <Interpreters/Context.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Storages/Output/WriteBufferBuilder.h>

namespace local_engine
{
class OutputFormatFile
{
public:
    struct OutputFormat
    {
    public:
        DB::OutputFormatPtr output;
        std::unique_ptr<DB::WriteBuffer> write_buffer;
    };
    using OutputFormatPtr = std::shared_ptr<OutputFormat>;

    OutputFormatFile(
        DB::ContextPtr context_,
        const std::string & file_uri_,
        WriteBufferBuilderPtr write_buffer_builder_,
        const std::vector<std::string> & preferred_column_names_);

    virtual ~OutputFormatFile() = default;

    virtual OutputFormatPtr createOutputFormat(const DB::Block & header_) = 0;

protected:
    DB::Block creatHeaderWithPreferredColumnNames(const DB::Block & header);

    DB::ContextPtr context;
    std::string file_uri;
    WriteBufferBuilderPtr write_buffer_builder;
    std::vector<std::string> preferred_column_names;
};
using OutputFormatFilePtr = std::shared_ptr<OutputFormatFile>;

class OutputFormatFileUtil
{
public:
    static OutputFormatFilePtr createFile(
        DB::ContextPtr context,
        WriteBufferBuilderPtr write_buffer_builder_,
        const std::string & file_uri_,
        const std::vector<std::string> & preferred_column_names,
        const std::string & format_hint = "");
};
}
