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
#include "OutputFormatFile.h"
#include <boost/algorithm/string/case_conv.hpp>
#include "ParquetOutputFormatFile.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}
}
namespace local_engine
{
OutputFormatFile::OutputFormatFile(DB::ContextPtr context_, const std::string & file_uri_, WriteBufferBuilderPtr write_buffer_builder_)
    : context(context_), file_uri(file_uri_), write_buffer_builder(write_buffer_builder_)
{
}

OutputFormatFilePtr OutputFormatFileUtil::createFile(
    DB::ContextPtr context, local_engine::WriteBufferBuilderPtr write_buffer_builder, const std::string & file_uri)
{
#if USE_PARQUET
    //TODO: can we support parquet file with suffix like .parquet1?
    if (boost::to_lower_copy(file_uri).ends_with(".parquet"))
    {
        return std::make_shared<ParquetOutputFormatFile>(context, file_uri, write_buffer_builder);
    }
#endif

    throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Format not supported for file :{}", file_uri);
}
}
