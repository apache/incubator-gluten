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


#include <Core/Block.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromFile.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Formats/Impl/NativeORCBlockInputFormat.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Common/DebugUtils.h>

using namespace DB;

int main()
{
    String path = "/data1/liyang/cppproject/spark/spark-3.3.2-bin-hadoop3/t_orc/data.orc";
    ReadBufferFromFile read_buffer(path);

    DataTypePtr string_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    DataTypes elem_types = {string_type, string_type};
    Strings elem_names = {"1", "2"};
    DataTypePtr tuple_type = std::make_shared<DataTypeTuple>(std::move(elem_types), std::move(elem_names));
    tuple_type = std::make_shared<DataTypeNullable>(std::move(tuple_type));

    Block header({
        {nullptr, tuple_type, "c"},
        // {nullptr, std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), "a"},
    });

    FormatSettings format_settings;
    InputFormatPtr format = std::make_shared<NativeORCBlockInputFormat>(read_buffer, header, format_settings);
    QueryPipeline pipeline(std::move(format));
    PullingPipelineExecutor reader(pipeline);
    Block block;
    reader.pull(block);
    debug::headBlock(block, 10);
    return 0;
}
