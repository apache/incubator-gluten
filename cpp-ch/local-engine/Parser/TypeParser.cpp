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
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parser/AggregateFunctionParser.h>
#include <Parser/FunctionParser.h>
#include <Parser/ParserContext.h>
#include <Parser/RelParsers/RelParser.h>
#include <Parser/SerializedPlanParser.h>
#include <Parser/TypeParser.h>
#include <Poco/StringTokenizer.h>
#include <Common/Exception.h>
#include <Common/QueryContext.h>

namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_TYPE;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}
}

namespace local_engine
{
using namespace DB;
std::unordered_map<String, String> TypeParser::type_names_mapping
    = {{"BooleanType", "UInt8"},
       {"ByteType", "Int8"},
       {"ShortType", "Int16"},
       {"IntegerType", "Int32"},
       {"LongType", "Int64"},
       {"FloatType", "Float32"},
       {"DoubleType", "Float64"},
       {"StringType", "String"},
       {"DateType", "Date32"},
       {"TimestampType", "DateTime64"}};

String TypeParser::getCHTypeName(const String & spark_type_name)
{
    if (startsWith(spark_type_name, "DecimalType"))
        return "Decimal" + spark_type_name.substr(strlen("DecimalType"));
    else
    {
        auto it = type_names_mapping.find(spark_type_name);
        if (it == type_names_mapping.end())
            throw DB::Exception(DB::ErrorCodes::UNKNOWN_TYPE, "Unsupported substrait type: {}", spark_type_name);
        return it->second;
    }
}

DB::DataTypePtr TypeParser::getCHTypeByName(const String & spark_type_name)
{
    auto ch_type_name = getCHTypeName(spark_type_name);
    return DB::DataTypeFactory::instance().get(ch_type_name);
}

DB::DataTypePtr TypeParser::parseType(const substrait::Type & substrait_type, std::list<String> * field_names)
{
    DB::DataTypePtr ch_type = nullptr;

    std::string_view field_name;
    if (field_names)
    {
        assert(!field_names->empty());
        field_name = field_names->front();
        field_names->pop_front();
    }

    if (substrait_type.has_bool_())
    {
        ch_type = DB::DataTypeFactory::instance().get("Bool");
        ch_type = tryWrapNullable(substrait_type.bool_().nullability(), ch_type);
    }
    else if (substrait_type.has_i8())
    {
        ch_type = std::make_shared<DB::DataTypeInt8>();
        ch_type = tryWrapNullable(substrait_type.i8().nullability(), ch_type);
    }
    else if (substrait_type.has_i16())
    {
        ch_type = std::make_shared<DB::DataTypeInt16>();
        ch_type = tryWrapNullable(substrait_type.i16().nullability(), ch_type);
    }
    else if (substrait_type.has_i32())
    {
        ch_type = std::make_shared<DB::DataTypeInt32>();
        ch_type = tryWrapNullable(substrait_type.i32().nullability(), ch_type);
    }
    else if (substrait_type.has_i64())
    {
        ch_type = std::make_shared<DB::DataTypeInt64>();
        ch_type = tryWrapNullable(substrait_type.i64().nullability(), ch_type);
    }
    else if (substrait_type.has_string())
    {
        ch_type = std::make_shared<DB::DataTypeString>();
        ch_type = tryWrapNullable(substrait_type.string().nullability(), ch_type);
    }
    else if (substrait_type.has_binary())
    {
        ch_type = std::make_shared<DB::DataTypeString>();
        ch_type = tryWrapNullable(substrait_type.binary().nullability(), ch_type);
    }
    else if (substrait_type.has_fixed_char())
    {
        const auto & fixed_char = substrait_type.fixed_char();
        ch_type = std::make_shared<DB::DataTypeFixedString>(fixed_char.length());
        ch_type = tryWrapNullable(fixed_char.nullability(), ch_type);
    }
    else if (substrait_type.has_fixed_binary())
    {
        const auto & fixed_binary = substrait_type.fixed_binary();
        ch_type = std::make_shared<DB::DataTypeFixedString>(fixed_binary.length());
        ch_type = tryWrapNullable(fixed_binary.nullability(), ch_type);
    }
    else if (substrait_type.has_fp32())
    {
        ch_type = std::make_shared<DB::DataTypeFloat32>();
        ch_type = tryWrapNullable(substrait_type.fp32().nullability(), ch_type);
    }
    else if (substrait_type.has_fp64())
    {
        ch_type = std::make_shared<DB::DataTypeFloat64>();
        ch_type = tryWrapNullable(substrait_type.fp64().nullability(), ch_type);
    }
    else if (substrait_type.has_timestamp())
    {
        ch_type = std::make_shared<DB::DataTypeDateTime64>(6);
        ch_type = tryWrapNullable(substrait_type.timestamp().nullability(), ch_type);
    }
    else if (substrait_type.has_date())
    {
        ch_type = std::make_shared<DB::DataTypeDate32>();
        ch_type = tryWrapNullable(substrait_type.date().nullability(), ch_type);
    }
    else if (substrait_type.has_decimal())
    {
        UInt32 precision = substrait_type.decimal().precision();
        UInt32 scale = substrait_type.decimal().scale();
        if (precision > DB::DataTypeDecimal128::maxPrecision())
            throw DB::Exception(DB::ErrorCodes::UNKNOWN_TYPE, "Spark doesn't support decimal type with precision {}", precision);
        ch_type = DB::createDecimal<DB::DataTypeDecimal>(precision, scale);
        ch_type = tryWrapNullable(substrait_type.decimal().nullability(), ch_type);
    }
    else if (substrait_type.has_struct_())
    {
        const auto & types = substrait_type.struct_().types();
        DB::DataTypes struct_field_types(types.size());
        DB::Strings struct_field_names;

        if (field_names)
        {
            /// Construct CH tuple type following the DFS rule.
            /// Refer to NamedStruct in https://github.com/oap-project/gluten/blob/main/cpp-ch/local-engine/proto/substrait/type.proto
            for (int i = 0; i < types.size(); ++i)
            {
                struct_field_names.push_back(field_names->front());
                struct_field_types[i] = parseType(types[i], field_names);
            }
        }
        else
        {
            /// Construct CH tuple type without DFS rule.
            for (int i = 0; i < types.size(); ++i)
                struct_field_types[i] = parseType(types[i]);

            const auto & names = substrait_type.struct_().names();
            for (const auto & name : names)
                if (!name.empty())
                    struct_field_names.push_back(name);
        }

        if (!struct_field_names.empty())
            ch_type = std::make_shared<DB::DataTypeTuple>(struct_field_types, struct_field_names);
        else
            ch_type = std::make_shared<DB::DataTypeTuple>(struct_field_types);

        ch_type = tryWrapNullable(substrait_type.struct_().nullability(), ch_type);
    }
    else if (substrait_type.has_list())
    {
        auto ch_nested_type = parseType(substrait_type.list().type());
        ch_type = std::make_shared<DB::DataTypeArray>(ch_nested_type);
        ch_type = tryWrapNullable(substrait_type.list().nullability(), ch_type);
    }
    else if (substrait_type.has_map())
    {
        if (substrait_type.map().key().has_nothing())
        {
            // special case
            ch_type = std::make_shared<DB::DataTypeMap>(std::make_shared<DB::DataTypeNothing>(), std::make_shared<DB::DataTypeNothing>());
            ch_type = tryWrapNullable(substrait_type.map().nullability(), ch_type);
        }
        else
        {
            auto ch_key_type = parseType(substrait_type.map().key());
            auto ch_val_type = parseType(substrait_type.map().value());
            ch_type = std::make_shared<DB::DataTypeMap>(ch_key_type, ch_val_type);
            ch_type = tryWrapNullable(substrait_type.map().nullability(), ch_type);
        }
    }
    else if (substrait_type.has_nothing())
    {
        ch_type = std::make_shared<DB::DataTypeNothing>();
        ch_type = tryWrapNullable(substrait::Type_Nullability::Type_Nullability_NULLABILITY_NULLABLE, ch_type);
    }
    else
        throw DB::Exception(DB::ErrorCodes::UNKNOWN_TYPE, "Spark doesn't support type {}", substrait_type.DebugString());

    /// TODO(taiyang-li): consider Time/IntervalYear/IntervalDay/TimestampTZ/UUID/VarChar/FixedBinary/UserDefined
    return ch_type;
}

DB::DataTypePtr TypeParser::resolveNothingTypeNullability(DB::DataTypePtr parsed_result_type, DB::DataTypePtr output_type)
{
    if (parsed_result_type->getTypeId() == output_type->getTypeId())
    {
        if (DB::isArray(parsed_result_type))
        {
            return std::make_shared<DB::DataTypeArray>(resolveNothingTypeNullability(
                typeid_cast<const DB::DataTypeArray *>(parsed_result_type.get())->getNestedType(),
                typeid_cast<const DB::DataTypeArray *>(output_type.get())->getNestedType()));
        }
        else if (DB::isMap(parsed_result_type))
        {
            const auto * from_map = typeid_cast<const DB::DataTypeMap *>(parsed_result_type.get());
            const auto * target_map = typeid_cast<const DB::DataTypeMap *>(output_type.get());
            auto from_key = from_map->getKeyType();
            auto target_key = target_map->getKeyType();
            auto from_value = from_map->getValueType();
            auto target_value = target_map->getValueType();
            auto key_type = resolveNothingTypeNullability(from_key, target_key);
            auto value_type = resolveNothingTypeNullability(from_value, target_value);
            return std::make_shared<DB::DataTypeMap>(key_type, value_type);
        }
        else if (DB::isTuple(parsed_result_type))
        {
            const auto * from_tuple = typeid_cast<const DB::DataTypeTuple *>(parsed_result_type.get());
            const auto * target_tuple = typeid_cast<const DB::DataTypeTuple *>(output_type.get());
            size_t from_size = from_tuple->getElements().size();
            size_t target_size = target_tuple->getElements().size();
            if (from_size != target_size)
                return output_type;

            DB::DataTypes elements(target_size);
            for (size_t i = 0; i < from_size; ++i)
            {
                elements[i] = resolveNothingTypeNullability(from_tuple->getElements()[i], target_tuple->getElements()[i]);
            }
            if (target_tuple->haveExplicitNames())
            {
                const auto & names = target_tuple->getElementNames();
                return std::make_shared<DB::DataTypeTuple>(elements, names);
            }
            else
                return std::make_shared<DB::DataTypeTuple>(elements);
        }
        else if (parsed_result_type->isNullable() && output_type->isNullable())
        {
            auto from_nested = typeid_cast<const DB::DataTypeNullable *>(parsed_result_type.get())->getNestedType();
            auto target_nested = typeid_cast<const DB::DataTypeNullable *>(output_type.get())->getNestedType();
            return std::make_shared<DB::DataTypeNullable>(resolveNothingTypeNullability(from_nested, target_nested));
        }
    }
    else if (parsed_result_type->isNullable() && !output_type->isNullable())
    {
        auto nested_type = typeid_cast<const DB::DataTypeNullable *>(parsed_result_type.get())->getNestedType();
        if (DB::isNothing(nested_type) && DB::isNothing(output_type))
            return std::make_shared<DB::DataTypeNullable>(output_type);
        else
            return resolveNothingTypeNullability(nested_type, output_type);
    }
    else if (!parsed_result_type->isNullable() && output_type->isNullable())
    {
        auto nested_type = typeid_cast<const DB::DataTypeNullable *>(output_type.get())->getNestedType();
        return std::make_shared<DB::DataTypeNullable>(resolveNothingTypeNullability(parsed_result_type, nested_type));
    }
    return output_type;
}


DB::Block TypeParser::buildBlockFromNamedStruct(const substrait::NamedStruct & struct_, const std::string & low_card_cols)
{
    std::unordered_set<std::string> low_card_columns;
    Poco::StringTokenizer tokenizer(low_card_cols, ",");
    for (const auto & token : tokenizer)
        low_card_columns.insert(token);

    DB::ColumnsWithTypeAndName internal_cols;
    internal_cols.reserve(struct_.names_size());
    std::list<std::string> field_names;
    for (int i = 0; i < struct_.names_size(); ++i)
        field_names.emplace_back(struct_.names(i));

    for (int i = 0; i < struct_.struct_().types_size(); ++i)
    {
        auto name = field_names.front();
        const auto & substrait_type = struct_.struct_().types(i);
        auto ch_type = parseType(substrait_type, &field_names);

        if (low_card_columns.contains(name))
            ch_type = std::make_shared<DB::DataTypeLowCardinality>(ch_type);

        // This is a partial aggregate data column.
        // It's type is special, must be a struct type contains all arguments types.
        // Notice: there are some coincidence cases in which the type is not a struct type, e.g. name is "_1#913 + _2#914#928". We need to handle it.
        Poco::StringTokenizer name_parts(name, "#");
        if (name_parts.count() >= 4 && !name.contains(' '))
        {
            auto nested_data_type = DB::removeNullable(ch_type);
            const auto * tuple_type = typeid_cast<const DB::DataTypeTuple *>(nested_data_type.get());
            if (!tuple_type)
                throw DB::Exception(DB::ErrorCodes::UNKNOWN_TYPE, "Tuple is expected, but got {}", ch_type->getName());

            auto args_types = tuple_type->getElements();
            AggregateFunctionProperties properties;
            auto tmp_ctx = DB::Context::createCopy(QueryContext::globalContext());
            auto parser_context = ParserContext::build(tmp_ctx);
            auto function_parser = AggregateFunctionParserFactory::instance().get(name_parts[3], parser_context);
            /// This may remove elements from args_types, because some of them are used to determine CH function name, but not needed for the following
            /// call `AggregateFunctionFactory::instance().get`
            auto agg_function_name = function_parser->getCHFunctionName(args_types);
            ch_type = RelParser::getAggregateFunction(
                          agg_function_name, args_types, properties, function_parser->getDefaultFunctionParameters())
                          ->getStateType();
        }

        internal_cols.push_back(ColumnWithTypeAndName(ch_type, name));
    }

    DB::Block res(std::move(internal_cols));
    return res;
}

DB::Block TypeParser::buildBlockFromNamedStructWithoutDFS(const substrait::NamedStruct & named_struct_)
{
    DB::ColumnsWithTypeAndName columns;
    const auto & names = named_struct_.names();
    const auto & types = named_struct_.struct_().types();
    columns.reserve(names.size());
    if (names.size() != types.size())
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Mismatch between names and types in named_struct");

    int size = named_struct_.names_size();
    for (int i = 0; i < size; ++i)
    {
        const auto & name = names[i];
        const auto & type = types[i];
        auto ch_type = parseType(type);
        columns.emplace_back(ColumnWithTypeAndName(ch_type, name));
    }

    DB::Block res(std::move(columns));
    return res;
}

bool TypeParser::isTypeMatched(const substrait::Type & substrait_type, const DataTypePtr & ch_type, bool ignore_nullability)
{
    const auto parsed_ch_type = TypeParser::parseType(substrait_type);
    if (ignore_nullability)
    {
        // if it's only different in nullability, we consider them same.
        // this will be problematic for some functions being not-null in spark but nullable in clickhouse.
        // e.g. murmur3hash
        const auto a = removeNullable(parsed_ch_type);
        const auto b = removeNullable(ch_type);
        return a->equals(*b);
    }
    else
        return parsed_ch_type->equals(*ch_type);
}

DB::DataTypePtr TypeParser::tryWrapNullable(substrait::Type_Nullability nullable, DB::DataTypePtr nested_type)
{
    if (nullable == substrait::Type_Nullability::Type_Nullability_NULLABILITY_NULLABLE && !nested_type->isNullable())
        return std::make_shared<DB::DataTypeNullable>(nested_type);
    return nested_type;
}
}
