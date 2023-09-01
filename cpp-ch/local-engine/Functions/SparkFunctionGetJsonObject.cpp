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
#include "SparkFunctionGetJsonObject.h"
#include <Columns/FormattedColumn.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/JSONPath/ASTs/ASTJSONPath.h>
#include <Functions/JSONPath/Generator/GeneratorJSONPath.h>
#include <Functions/JSONPath/Parsers/ParserJSONPath.h>
#include <Common/Exception.h>
#include <Common/JSONParsers/DummyJSONParser.h>
#include <Common/JSONParsers/RapidJSONParser.h>
#include <Common/JSONParsers/SimdJSONParser.h>

#include <Interpreters/Context.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <Parsers/IParser.h>
#include <Parsers/Lexer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <base/range.h>
#include <Columns/DecodedJSONColumn.h>
#include <Functions/FunctionSQLJSON.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}
}

namespace local_engine
{

class FunctionSQLDecodedJSONHelpers
{
public:
    template <typename Name, template <typename> typename Impl, class JSONParser>
    class Executor
    {
    public:
        static DB::ColumnPtr run(const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr & result_type, size_t input_rows_count, uint32_t parse_depth, const DB::ContextPtr & context)
        {
            DB::MutableColumnPtr to{result_type->createColumn()};
            to->reserve(input_rows_count);

            if (arguments.size() < 2)
            {
                throw DB::Exception(DB::ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION, "JSONPath functions require at least 2 arguments");
            }

            const auto & json_column = arguments[0];
            const auto * decoded_json_column = castToDecodedJSONColumn<JSONParser>(json_column.column);
            if (!decoded_json_column)
            {
                throw DB::Exception(DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                "JSONPath functions require first argument to be JSON of string, illegal type: {}",
                                json_column.column->getName());
            }

            const auto & json_path_column = arguments[1];

            if (!isString(json_path_column.type))
            {
                throw DB::Exception(DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                "JSONPath functions require second argument "
                                "to be JSONPath of type string, illegal type: {}", json_path_column.type->getName());
            }
            if (!isColumnConst(*json_path_column.column))
            {
                throw DB::Exception(DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument (JSONPath) must be constant string");
            }

            const DB::ColumnPtr & arg_jsonpath = json_path_column.column;
            const auto * arg_jsonpath_const = typeid_cast<const DB::ColumnConst *>(arg_jsonpath.get());
            const auto * arg_jsonpath_string = typeid_cast<const DB::ColumnString *>(arg_jsonpath_const->getDataColumnPtr().get());


            /// Get data and offsets for 1 argument (JSONPath)
            const DB::ColumnString::Chars & chars_path = arg_jsonpath_string->getChars();
            const DB::ColumnString::Offsets & offsets_path = arg_jsonpath_string->getOffsets();

            /// Prepare to parse 1 argument (JSONPath)
            const char * query_begin = reinterpret_cast<const char *>(&chars_path[0]);
            const char * query_end = query_begin + offsets_path[0] - 1;

            /// Tokenize query
            DB::Tokens tokens(query_begin, query_end);
            /// Max depth 0 indicates that depth is not limited
            DB::IParser::Pos token_iterator(tokens, parse_depth);

            /// Parse query and create AST tree
            DB::Expected expected;
            DB::ASTPtr res;
            DB::ParserJSONPath parser;
            const bool parse_res = parser.parse(token_iterator, res, expected);
            if (!parse_res)
            {
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unable to parse JSONPath");
            }

            Impl<JSONParser> impl;
            for (const auto i : collections::range(0, input_rows_count))
            {
                auto element = decoded_json_column->getDecodedElement(i);
                if (!element) [[unlikely]]
                {
                    to->insertDefault();
                }
                else
                {
                    auto added_col = impl.insertResultToColumn(*to, element->document, res, context);
                    if (!added_col)
                    {
                        to->insertDefault();
                    }
                }
            }
            return to;
        }
    };
};

template <typename Name, template <typename> typename Impl>
class ExtendedFunctionSQLJSON : public DB::IFunction, DB::WithConstContext
{
public:
    static DB::FunctionPtr create(DB::ContextPtr context_) { return std::make_shared<ExtendedFunctionSQLJSON>(context_); }
    explicit ExtendedFunctionSQLJSON(DB::ContextPtr context_) : DB::WithConstContext(context_) { }

    static constexpr auto name = Name::name;
    String getName() const override { return Name::name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }
    DB::ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }
    bool isSuitableForShortCircuitArgumentsExecution(const DB::DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DB::DataTypePtr getReturnTypeImpl(const DB::ColumnsWithTypeAndName & arguments) const override
    {
        return Impl<DB::DummyJSONParser>::getReturnType(Name::name, arguments, getContext());
    }

    DB::ColumnPtr executeImpl(const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr & result_type, size_t input_rows_count) const override
    {
        /// Choose JSONParser.
        /// 1. Lexer(path) -> Tokens
        /// 2. Create ASTPtr
        /// 3. Parser(Tokens, ASTPtr) -> complete AST
        /// 4. Execute functions: call getNextItem on generator and handle each item
        unsigned parse_depth = static_cast<unsigned>(getContext()->getSettingsRef().max_parser_depth);
        if (typeid_cast<const DB::ColumnString *>(arguments[0].column.get()))
        {
#if USE_SIMDJSON
            if (getContext()->getSettingsRef().allow_simdjson)
                return DB::FunctionSQLJSONHelpers::Executor<Name, Impl, DB::SimdJSONParser>::run(
                    arguments, result_type, input_rows_count, parse_depth, getContext());
#endif
            return DB::FunctionSQLJSONHelpers::Executor<Name, Impl, DB::DummyJSONParser>::run(
                arguments, result_type, input_rows_count, parse_depth, getContext());
        }
        else
        {
#if USE_SIMDJSON
            if (getContext()->getSettingsRef().allow_simdjson && castToDecodedJSONColumn<DB::SimdJSONParser>(arguments[0].column))
            {
                return FunctionSQLDecodedJSONHelpers::Executor<Name, Impl, DB::SimdJSONParser>::run(
                    arguments, result_type, input_rows_count, parse_depth, getContext());
            }
#endif
            return FunctionSQLDecodedJSONHelpers::Executor<Name, Impl, DB::DummyJSONParser>::run(
                arguments, result_type, input_rows_count, parse_depth, getContext());
        }
    }
};

REGISTER_FUNCTION(GetJsonObject)
{
    factory.registerFunction<ExtendedFunctionSQLJSON<GetJsonObject, GetJsonObjectImpl>>();
}
}
