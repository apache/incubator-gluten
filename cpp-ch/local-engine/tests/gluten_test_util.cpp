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
#include "gluten_test_util.h"
#include <filesystem>
#include <sstream>

#include <Formats/FormatSettings.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromFile.h>

#include <Interpreters/ActionsVisitor.h>
#include <Parser/SerializedPlanParser.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <Processors/Formats/Impl/ParquetBlockInputFormat.h>
#include <Common/Exception.h>

namespace fs = std::filesystem;

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace local_engine::test
{
using namespace DB;
ActionsDAGPtr parseFilter(const std::string & filter, const AnotherRowType & name_and_types)
{
    using namespace DB;

    std::unordered_map<std::string, ColumnWithTypeAndName> node_name_to_input_column;
    std::ranges::transform(
        name_and_types,
        std::inserter(node_name_to_input_column, node_name_to_input_column.end()),
        [](const auto & name_and_type) { return std::make_pair(name_and_type.name, toBlockFieldType(name_and_type)); });

    NamesAndTypesList aggregation_keys;
    ColumnNumbersList aggregation_keys_indexes_list;
    const AggregationKeysInfo info(aggregation_keys, aggregation_keys_indexes_list, GroupByKind::NONE);
    constexpr SizeLimits size_limits_for_set;
    ParserExpression parser2;
    const ASTPtr ast_exp = parseQuery(parser2, filter.data(), filter.data() + filter.size(), "", 0, 0, 0);
    const auto prepared_sets = std::make_shared<PreparedSets>();
    ActionsMatcher::Data visitor_data(
        SerializedPlanParser::global_context,
        size_limits_for_set,
        static_cast<size_t>(0),
        name_and_types,
        std::make_shared<ActionsDAG>(name_and_types),
        prepared_sets /* prepared_sets */,
        false /* no_subqueries */,
        false /* no_makeset */,
        false /* only_consts */,
        info);
    ActionsVisitor(visitor_data).visit(ast_exp);
    return ActionsDAG::buildFilterActionsDAG({visitor_data.getActions()->getOutputs().back()}, node_name_to_input_column);
}

const char * get_data_dir()
{
    const auto * const result = std::getenv("PARQUET_TEST_DATA");
    if (result == nullptr || result[0] == 0)
    {
        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR, "Please point the PARQUET_TEST_DATA environment variable to the test data directory");
    }
    return result;
}

std::string data_file(const char * file)
{
    const fs::path parquet_path = file;
    if (parquet_path.is_absolute())
        return file;
    const std::string dir_string(get_data_dir());
    std::stringstream ss;
    ss << dir_string << "/" << file;
    return ss.str();
}

std::shared_ptr<arrow::io::RandomAccessFile> asArrowFileForParquet(DB::ReadBuffer & in, const DB::FormatSettings & settings)
{
    std::atomic<int> is_stopped{0};
    return asArrowFile(in, settings, is_stopped, "Parquet", PARQUET_MAGIC_BYTES, /* avoid_buffering */ true);
}

DB::DataTypePtr toDataType(const parquet::ColumnDescriptor & type)
{
    switch (type.physical_type())
    {
        case parquet::Type::BOOLEAN:
            break;
        case parquet::Type::INT32:
            switch (type.converted_type())
            {
                case parquet::ConvertedType::NONE:
                    return INT();
                case parquet::ConvertedType::UINT_8:
                    return UINT8();
                case parquet::ConvertedType::UINT_16:
                    return UINT16();
                case parquet::ConvertedType::UINT_32:
                    return UINT();
                case parquet::ConvertedType::INT_8:
                    return INT8();
                case parquet::ConvertedType::INT_16:
                    return INT16();
                case parquet::ConvertedType::INT_32:
                    return INT();
                default:
                    break;
            }
            break;
        case parquet::Type::INT64:
            switch (type.converted_type())
            {
                case parquet::ConvertedType::NONE:
                case parquet::ConvertedType::INT_64:
                    return BIGINT();
                case parquet::ConvertedType::UINT_64:
                    return UBIGINT();
                default:
                    break;
            }
            break;
        case parquet::Type::INT96:
            break;
        case parquet::Type::FLOAT:
            break;
        case parquet::Type::DOUBLE:
            switch (type.converted_type())
            {
                case parquet::ConvertedType::NONE:
                    return DOUBLE();
                default:
                    break;
            }
            break;
        case parquet::Type::BYTE_ARRAY:
            switch (type.converted_type())
            {
                case parquet::ConvertedType::UTF8:
                    return STRING();
                default:
                    break;
            }
            break;
        case parquet::Type::FIXED_LEN_BYTE_ARRAY:
            break;
        case parquet::Type::UNDEFINED:
            break;
    }
    assert(false);
}

AnotherRowType readParquetSchema(const std::string & file)
{
    DB::FormatSettings settings;
    const auto in = std::make_shared<DB::ReadBufferFromFile>(file);
    DB::ParquetSchemaReader schema_reader(*in, settings);
    return schema_reader.readSchema();
}
}
