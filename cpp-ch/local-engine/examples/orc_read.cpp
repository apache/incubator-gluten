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