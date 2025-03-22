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

#include "FunctionParser.h"
#include <memory>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Parser/TypeParser.h>
#include "Common/Exception.h"
#include <Common/BlockTypeUtils.h>
#include <Common/CHUtil.h>
#include "ExpressionParser.h"

#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UNKNOWN_FUNCTION;
extern const int NOT_IMPLEMENTED;
}
}

namespace local_engine
{
using namespace DB;

FunctionParser::FunctionParser(ParserContextPtr ctx)
    : parser_context(ctx)
{
    expression_parser = std::make_unique<ExpressionParser>(parser_context);
}

String FunctionParser::getCHFunctionName(const substrait::Expression_ScalarFunction & substrait_func) const
{
    // no meaning
    /// There is no any simple equivalent ch function.
    return "";
}

String FunctionParser::getUniqueName(const String & name) const
{
    return expression_parser->getUniqueName(name);
}


const DB::ActionsDAG::Node *
FunctionParser::addColumnToActionsDAG(DB::ActionsDAG & actions_dag, const DB::DataTypePtr & type, const DB::Field & field) const
{
    return expression_parser->addConstColumn(actions_dag, type, field);
}

const DB::ActionsDAG::Node *
FunctionParser::toFunctionNode(DB::ActionsDAG & action_dag, const String & func_name, const DB::ActionsDAG::NodeRawConstPtrs & args) const
{
    return expression_parser->toFunctionNode(action_dag, func_name, args);
}

const DB::ActionsDAG::Node * FunctionParser::toFunctionNode(
    DB::ActionsDAG & action_dag, const String & func_name, const String & result_name, const DB::ActionsDAG::NodeRawConstPtrs & args) const
{
    return expression_parser->toFunctionNode(action_dag, func_name, args, result_name);
}

const ActionsDAG::Node * FunctionParser::parseFunctionWithDAG(
    const substrait::Expression & rel, std::string & result_name, DB::ActionsDAG & actions_dag, bool keep_result) const
{
    const auto * node = expression_parser->parseFunction(rel.scalar_function(), actions_dag, keep_result);
    result_name = node->result_name;
    return node;
}

const DB::ActionsDAG::Node * FunctionParser::parseExpression(DB::ActionsDAG & actions_dag, const substrait::Expression & rel) const
{
    return expression_parser->parseExpression(actions_dag, rel);
}

std::pair<DataTypePtr, Field> FunctionParser::parseLiteral(const substrait::Expression_Literal & literal) const
{
    return LiteralParser::parse(literal);
}

ActionsDAG::NodeRawConstPtrs
FunctionParser::parseFunctionArguments(const substrait::Expression_ScalarFunction & substrait_func, ActionsDAG & actions_dag) const
{
    ActionsDAG::NodeRawConstPtrs parsed_args;
    return expression_parser->parseFunctionArguments(actions_dag, substrait_func);
}


const ActionsDAG::Node * FunctionParser::parse(const substrait::Expression_ScalarFunction & substrait_func, ActionsDAG & actions_dag) const
{
    auto ch_func_name = getCHFunctionName(substrait_func);
    auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
    const auto * func_node = toFunctionNode(actions_dag, ch_func_name, parsed_args);
    return convertNodeTypeIfNeeded(substrait_func, func_node, actions_dag);
}

// Nothing type doesn't have nullability in substrait, but CH cares this.
// Before this, we presumed that all `nothing` types were nullable. However, this assumption resulted in the failure of following query.
//   select array() from t union all select array(123) from t;
// There is a conversion from Array(Nullable(Nothing)) to Array(UInt32) in it.
// Additionally, if we do not encapsulate the nothing type within nullable, the subsequent query fails.
//   select cast(array(null) as String)
// Therefore, when the output type in the Substrait plan is nothing, we ascertain the nullability of the output type based on the result
// type of the actions node.
DB::DataTypePtr FunctionParser::correctNothingTypeNullability(const DB::DataTypePtr & from_type, const DB::DataTypePtr & target_type) const
{
    if (from_type->getTypeId() == target_type->getTypeId())
    {
        if (DB::isArray(from_type))
        {
            auto res = std::make_shared<DB::DataTypeArray>(correctNothingTypeNullability(
                typeid_cast<const DB::DataTypeArray *>(from_type.get())->getNestedType(),
                typeid_cast<const DB::DataTypeArray *>(target_type.get())->getNestedType()));
            return res;
        }
        else if (DB::isMap(from_type))
        {
            const auto * from_map = typeid_cast<const DB::DataTypeMap *>(from_type.get());
            const auto * target_map = typeid_cast<const DB::DataTypeMap *>(target_type.get());
            auto from_key = from_map->getKeyType();
            auto target_key = target_map->getKeyType();
            auto from_value = from_map->getValueType();
            auto target_value = target_map->getValueType();
            auto key_type = correctNothingTypeNullability(from_key, target_key);
            auto value_type = correctNothingTypeNullability(from_value, target_value);
            return std::make_shared<DB::DataTypeMap>(key_type, value_type);
        }
        else if (DB::isTuple(from_type))
        {
            const auto * from_tuple = typeid_cast<const DB::DataTypeTuple *>(from_type.get());
            const auto * target_tuple = typeid_cast<const DB::DataTypeTuple *>(target_type.get());
            size_t from_size = from_tuple->getElements().size();
            size_t target_size = target_tuple->getElements().size();
            if (from_size != target_size)
                return target_type;

            DB::DataTypes elements(target_size);
            for (size_t i = 0; i < from_size; ++i)
            {
                elements[i] = correctNothingTypeNullability(from_tuple->getElements()[i], target_tuple->getElements()[i]);
            }
            if (target_tuple->haveExplicitNames())
            {
                const auto & names = target_tuple->getElementNames();
                return std::make_shared<DB::DataTypeTuple>(elements, names);
            }
            else
                return std::make_shared<DB::DataTypeTuple>(elements);
        }
        else if (from_type->isNullable() && target_type->isNullable())
        {
            auto from_nested = typeid_cast<const DB::DataTypeNullable *>(from_type.get())->getNestedType();
            auto target_nested = typeid_cast<const DB::DataTypeNullable *>(target_type.get())->getNestedType();
            return std::make_shared<DB::DataTypeNullable>(correctNothingTypeNullability(from_nested, target_nested));
        }
        else
            return target_type;
    }
    else if (from_type->isNullable() && !target_type->isNullable())
    {
        auto from_nested = typeid_cast<const DB::DataTypeNullable *>(from_type.get())->getNestedType();
        if (DB::isNothing(from_nested) && DB::isNothing(target_type))
        {
            return from_type;
        }
        else
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Convert nullable type to non-nullable type is not supported");
    }
    else if (!from_type->isNullable() && target_type->isNullable())
    {
        auto target_nested = typeid_cast<const DB::DataTypeNullable *>(target_type.get())->getNestedType();
        return std::make_shared<DB::DataTypeNullable>(correctNothingTypeNullability(from_type, target_nested));
    }
    return target_type;
}

const ActionsDAG::Node * FunctionParser::convertNodeTypeIfNeeded(
    const substrait::Expression_ScalarFunction & substrait_func, const ActionsDAG::Node * func_node, ActionsDAG & actions_dag) const
{
    const auto & output_type = substrait_func.output_type();
    const ActionsDAG::Node * result_node = nullptr;

    auto convert_type_if_needed = [&]()
    {
        auto result_type = TypeParser::parseType(output_type);
        result_type = correctNothingTypeNullability(func_node->result_type, result_type);
        if (!result_type->equals(*func_node->result_type))
        {
            if (DB::isDecimalOrNullableDecimal(result_type))
            {
                return ActionsDAGUtil::convertNodeType(
                    actions_dag,
                    func_node,
                    // as stated in isTypeMatched， currently we don't change nullability of the result type
                    func_node->result_type->isNullable() ? local_engine::wrapNullableType(true, result_type)
                                                         : local_engine::removeNullable(result_type),
                    func_node->result_name,
                    CastType::accurateOrNull);
            }
            else
            {
                return ActionsDAGUtil::convertNodeType(
                    actions_dag,
                    func_node,
                    // as stated in isTypeMatched， currently we don't change nullability of the result type
                    func_node->result_type->isNullable() ? local_engine::wrapNullableType(true, TypeParser::parseType(output_type))
                                                         : DB::removeNullable(TypeParser::parseType(output_type)),
                    func_node->result_name);
            }
        }
        else
            return func_node;
    };
    result_node = convert_type_if_needed();

    /// Notice that in CH Bool and UInt8 have different serialization and deserialization methods, which will cause issue when executing cast(bool as string) in spark in spark.
    auto convert_uint8_to_bool_if_needed = [&]() -> const auto *
    {
        auto * mutable_result_node = const_cast<ActionsDAG::Node *>(result_node);
        auto denull_result_type = DB::removeNullable(result_node->result_type);
        if (isUInt8(denull_result_type) && output_type.has_bool_())
        {
            auto bool_type = DB::DataTypeFactory::instance().get("Bool");
            if (result_node->result_type->isNullable())
                bool_type = DB::makeNullable(bool_type);

            mutable_result_node->result_type = std::move(bool_type);
            return mutable_result_node;
        }
        else
            return result_node;
    };
    result_node = convert_uint8_to_bool_if_needed();

    return result_node;
}

void FunctionParserFactory::registerFunctionParser(const String & name, Value value)
{
    if (!parsers.emplace(name, value).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FunctionParserFactory: function parser name '{}' is not unique", name);
}

FunctionParserPtr FunctionParserFactory::get(const String & name, ParserContextPtr ctx)
{
    auto res = tryGet(name, ctx);
    if (!res)
        throw Exception(ErrorCodes::UNKNOWN_FUNCTION, "Unknown function parser {}", name);

    return res;
}

FunctionParserPtr FunctionParserFactory::tryGet(const String & name, ParserContextPtr ctx)
{
    auto it = parsers.find(name);
    if (it != parsers.end())
    {
        auto creator = it->second;
        return creator(ctx);
    }
    else
        return {};
}

FunctionParserFactory & FunctionParserFactory::instance()
{
    static FunctionParserFactory factory;
    return factory;
}

}
