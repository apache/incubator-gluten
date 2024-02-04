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
#include "config.h"

#if USE_PARQUET

#include <ranges>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromFile.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Processors/Formats/Impl/ParquetBlockInputFormat.h>
#include <QueryPipeline/QueryPipeline.h>
#include <gtest/gtest.h>
#include <parquet/arrow/reader.h>
#include <parquet/column_reader.h>
#include <parquet/level_conversion.h>
#include <Common/DebugUtils.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}
}

namespace test
{
const char * get_data_dir()
{
    const auto result = std::getenv("PARQUET_TEST_DATA");
    if (!result || !result[0])
    {
        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR, "Please point the PARQUET_TEST_DATA environment variable to the test data directory");
    }
    return result;
}

std::string data_file(const char * file)
{
    std::string dir_string(test::get_data_dir());
    std::stringstream ss;
    ss << dir_string << "/" << file;
    return ss.str();
}

parquet::internal::LevelInfo ComputeLevelInfo(const parquet::ColumnDescriptor * descr)
{
    parquet::internal::LevelInfo level_info;
    level_info.def_level = descr->max_definition_level();
    level_info.rep_level = descr->max_repetition_level();

    int16_t min_spaced_def_level = descr->max_definition_level();
    const ::parquet::schema::Node * node = descr->schema_node().get();
    while (node != nullptr && !node->is_repeated())
    {
        if (node->is_optional())
            min_spaced_def_level--;
        node = node->parent();
    }
    level_info.repeated_ancestor_def_level = min_spaced_def_level;
    return level_info;
}
}
using namespace DB;

template <class SchemaReader>
static void readSchema(const String & path)
{
    FormatSettings settings;
    auto in = std::make_shared<ReadBufferFromFile>(test::data_file(path.c_str()));
    SchemaReader schema_reader(*in, settings);
    auto name_and_types = schema_reader.readSchema();
    auto & factory = DataTypeFactory::instance();

    auto check_type = [&name_and_types, &factory](const String & column, const String & expect_str_type)
    {
        auto expect_type = factory.get(expect_str_type);

        auto name_and_type = name_and_types.tryGetByName(column);
        EXPECT_TRUE(name_and_type);

        EXPECT_TRUE(name_and_type->type->equals(*expect_type))
            << "real_type:" << name_and_type->type->getName() << ", expect_type:" << expect_type->getName();
    };

    check_type("f_bool", "Nullable(UInt8)");
    check_type("f_byte", "Nullable(Int8)");
    check_type("f_short", "Nullable(Int16)");
    check_type("f_int", "Nullable(Int32)");
    check_type("f_long", "Nullable(Int64)");
    check_type("f_float", "Nullable(Float32)");
    check_type("f_double", "Nullable(Float64)");
    check_type("f_string", "Nullable(String)");
    check_type("f_binary", "Nullable(String)");
    check_type("f_decimal", "Nullable(Decimal(10, 2))");
    check_type("f_date", "Nullable(Date32)");
    check_type("f_timestamp", "Nullable(DateTime64(9))");
    //    check_type("f_array", "Nullable(Array(Nullable(String)))");
    //    check_type("f_array_array", "Nullable(Array(Nullable(Array(Nullable(String)))))");
    //    check_type("f_array_map", "Nullable(Array(Nullable(Map(String, Nullable(Int64)))))");
    //    check_type("f_array_struct", "Nullable(Array(Nullable(Tuple(a Nullable(String), b Nullable(Int64)))))");
    //    check_type("f_map", "Nullable(Map(String, Nullable(Int64)))");
    //    check_type("f_map_map", "Nullable(Map(String, Nullable(Map(String, Nullable(Int64)))))");
    //    check_type("f_map_array", "Nullable(Map(String, Nullable(Array(Nullable(Int64)))))");
    //    check_type("f_map_struct", "Nullable(Map(String, Nullable(Tuple(a Nullable(String), b Nullable(Int64)))))");
    //    check_type("f_struct", "Nullable(Tuple(a Nullable(String), b Nullable(Int64)))");
    //    check_type(
    //        "f_struct_struct",
    //        "Nullable(Tuple(a Nullable(String), b Nullable(Int64), c Nullable(Tuple(x Nullable(String), y Nullable(Int64)))))");
    //    check_type("f_struct_array", "Nullable(Tuple(a Nullable(String), b Nullable(Int64), c Nullable(Array(Nullable(Int64)))))");
    //    check_type("f_struct_map", "Nullable(Tuple(a Nullable(String), b Nullable(Int64), c Nullable(Map(String, Nullable(Int64)))))");
}


template <class SchemaReader>
static ColumnsWithTypeAndName
createColumn(const String & full_path, const FormatSettings & settings, const std::map<String, Field> & fields)
{
    ReadBufferFromFile in(full_path);
    SchemaReader schema_reader(in, settings);
    auto name_and_types = schema_reader.readSchema();

    ColumnsWithTypeAndName columns;
    columns.reserve(name_and_types.size());
    auto is_selected = [&fields](const auto & name_and_type) { return fields.contains(name_and_type.name); };
    auto column_type_name = [](const auto & name_and_type) { return ColumnWithTypeAndName(name_and_type.type, name_and_type.name); };
    auto view = name_and_types | std::views::filter(is_selected) | std::views::transform(column_type_name);
    std::ranges::copy(view, std::back_inserter(columns));
    return columns;
}

template <class SchemaReader, class InputFormat>
static void readData(const String & path, const std::map<String, Field> & fields)
{
    String full_path = test::data_file(path.c_str());
    FormatSettings settings;
    ColumnsWithTypeAndName columns = createColumn<SchemaReader>(full_path, settings, fields);
    Block header(columns);
    ReadBufferFromFile in(full_path);

    InputFormatPtr format;
    if constexpr (std::is_same_v<InputFormat, DB::ParquetBlockInputFormat>)
        format = std::make_shared<InputFormat>(in, header, settings, 1, 8192);
    else
        format = std::make_shared<InputFormat>(in, header, settings);

    QueryPipeline pipeline(format);
    PullingPipelineExecutor reader(pipeline);

    Block block;
    EXPECT_TRUE(reader.pull(block));
    EXPECT_TRUE(block.rows() == 1);

    for (const auto & column_header : columns)
    {
        const auto & name = column_header.name;
        auto it = fields.find(name);
        if (it != fields.end())
        {
            const auto & column = block.getByName(name);
            auto field = (*column.column)[0];
            auto expect_field = it->second;
            EXPECT_TRUE(field == expect_field) << "field:" << toString(field) << ", expect_field:" << toString(expect_field);
        }
    }
}

TEST(ParquetRead, ReadSchema)
{
    readSchema<ParquetSchemaReader>("alltypes/alltypes_notnull.parquet");
    readSchema<ParquetSchemaReader>("alltypes/alltypes_null.parquet");
}

TEST(ParquetRead, ReadDataNotNull)
{
    const std::map<String, Field> fields{
        {"f_array", Array{"hello", "world"}},
        {"f_bool", UInt8(1)},
        {"f_byte", Int8(1)},
        {"f_short", Int16(2)},
        {"f_int", Int32(3)},
        {"f_long", Int64(4)},
        {"f_float", Float32(5.5)},
        {"f_double", Float64(6.6)},
        {"f_string", "hello world"},
        {"f_binary", "hello world"},
        {"f_decimal", DecimalField<Decimal64>(777, 2)},
        {"f_date", Int32(18262)},
        {"f_timestamp", DecimalField<DateTime64>(1666162060000000L, 6)},
        {"f_array", Array{"hello", "world"}},
        {
            "f_array_array",
            []() -> Field
            {
                Array res;
                res.push_back(Array{"hello"});
                res.push_back(Array{"world"});
                return std::move(res);
            }(),
        },
        {
            "f_array_map",
            []() -> Field
            {
                Array res;

                Map map;
                map.push_back(Tuple{"hello", Int64(1)});
                res.push_back(map);

                map.clear();
                map.push_back(Tuple{"world", Int64(2)});
                res.push_back(map);

                return std::move(res);
            }(),
        },
        {
            "f_array_struct",
            []() -> Field
            {
                Array res;
                res.push_back(Tuple{"hello", Int64(1)});
                res.push_back(Tuple{"world", Int64(2)});

                return std::move(res);
            }(),
        },
        {
            "f_map",
            []() -> Field
            {
                Map res;
                res.push_back(Tuple{"hello", Int64(1)});
                res.push_back(Tuple{"world", Int64(2)});
                return std::move(res);
            }(),
        },
        {
            "f_map_map",
            []() -> Field
            {
                Map nested_map;
                nested_map.push_back(Tuple{"world", Int64(3)});

                Map res;
                res.push_back(Tuple{"hello", std::move(nested_map)});
                return std::move(res);
            }(),
        },
        {
            "f_map_array",
            []() -> Field
            {
                Array array{Int64(1), Int64(2), Int64(3)};

                Map res;
                res.push_back(Tuple{"hello", std::move(array)});
                return std::move(res);
            }(),
        },
        {
            "f_map_struct",
            []() -> Field
            {
                Tuple tuple{"world", Int64(4)};

                Map res;
                res.push_back(Tuple{"hello", std::move(tuple)});
                return std::move(res);
            }(),
        },
        {
            "f_struct",
            []() -> Field
            {
                Tuple res{"hello world", Int64(5)};
                return std::move(res);
            }(),
        },
        {
            "f_struct_struct",
            []() -> Field
            {
                Tuple tuple{"world", Int64(6)};
                Tuple res{"hello", Int64(6), std::move(tuple)};
                return std::move(res);
            }(),
        },
        {
            "f_struct_array",
            []() -> Field
            {
                Array array{Int64(1), Int64(2), Int64(3)};
                Tuple res{"hello", Int64(7), std::move(array)};
                return std::move(res);
            }(),
        },
        {
            "f_struct_map",
            []() -> Field
            {
                Map map;
                map.push_back(Tuple{"world", Int64(9)});

                Tuple res{"hello", Int64(8), std::move(map)};
                return std::move(res);
            }(),
        },
    };

    readData<ParquetSchemaReader, ParquetBlockInputFormat>("alltypes/alltypes_notnull.parquet", fields);
}


TEST(ParquetRead, ReadDataNull)
{
    GTEST_SKIP();
    std::map<String, Field> fields{
        {"f_array", Null{}},        {"f_bool", Null{}},   {"f_byte", Null{}},          {"f_short", Null{}},
        {"f_int", Null{}},          {"f_long", Null{}},   {"f_float", Null{}},         {"f_double", Null{}},
        {"f_string", Null{}},       {"f_binary", Null{}}, {"f_decimal", Null{}},       {"f_date", Null{}},
        {"f_timestamp", Null{}},    {"f_array", Null{}},  {"f_array_array", Null{}},   {"f_array_map", Null{}},
        {"f_array_struct", Null{}}, {"f_map", Null{}},    {"f_map_map", Null{}},       {"f_map_array", Null{}},
        {"f_map_struct", Null{}},   {"f_struct", Null{}}, {"f_struct_struct", Null{}}, {"f_struct_array", Null{}},
        {"f_struct_map", Null{}},
    };

    readData<ParquetSchemaReader, ParquetBlockInputFormat>("alltypes/alltypes_null.parquet", fields);
}
#endif
