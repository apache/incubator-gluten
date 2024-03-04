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

#include "config.h"

#include <memory>
#include <IO/WriteBuffer.h>
#include <Storages/Output/OutputFormatFile.h>

namespace local_engine
{
class TextOutputFormatFile : public OutputFormatFile
{
public:
    explicit TextOutputFormatFile(
        DB::ContextPtr context_,
        const std::string & file_uri_,
        WriteBufferBuilderPtr write_buffer_builder_,
        const std::vector<std::string> & preferred_column_names_);
    ~TextOutputFormatFile() override = default;

    OutputFormatFile::OutputFormatPtr createOutputFormat(const DB::Block & header) override;
};

}
