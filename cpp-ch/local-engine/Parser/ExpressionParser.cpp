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
#include "ExpressionParser.h"
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeSet.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/getLeastSupertype.h>
#include <IO/WriteBufferFromString.h>
#include <Parser/FunctionParser.h>
#include <Parser/ParserContext.h>
#include <Parser/SerializedPlanParser.h>
#include <Parser/TypeParser.h>
#include <Common/BlockTypeUtils.h>
#include <Common/CHUtil.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_FUNCTION;
extern const int UNKNOWN_TYPE;
extern const int BAD_ARGUMENTS;
}
}

namespace local_engine
{
std::pair<DB::DataTypePtr, DB::Field> LiteralParser::parse(const substrait::Expression_Literal & literal)
{
    DB::DataTypePtr type;
    DB::Field field;

    switch (literal.literal_type_case())
    {
        case substrait::Expression_Literal::kFp64: {
            type = std::make_shared<DB::DataTypeFloat64>();
            field = literal.fp64();
            break;
        }
        case substrait::Expression_Literal::kFp32: {
            type = std::make_shared<DB::DataTypeFloat32>();
            field = literal.fp32();
            break;
        }
        case substrait::Expression_Literal::kString: {
            type = std::make_shared<DB::DataTypeString>();
            field = literal.string();
            break;
        }
        case substrait::Expression_Literal::kBinary: {
            type = std::make_shared<DB::DataTypeString>();
            field = literal.binary();
            break;
        }
        case substrait::Expression_Literal::kI64: {
            type = std::make_shared<DB::DataTypeInt64>();
            field = literal.i64();
            break;
        }
        case substrait::Expression_Literal::kI32: {
            type = std::make_shared<DB::DataTypeInt32>();
            field = literal.i32();
            break;
        }
        case substrait::Expression_Literal::kBoolean: {
            type = DB::DataTypeFactory::instance().get("Bool");
            field = literal.boolean() ? UInt8(1) : UInt8(0);
            break;
        }
        case substrait::Expression_Literal::kI16: {
            type = std::make_shared<DB::DataTypeInt16>();
            field = literal.i16();
            break;
        }
        case substrait::Expression_Literal::kI8: {
            type = std::make_shared<DB::DataTypeInt8>();
            field = literal.i8();
            break;
        }
        case substrait::Expression_Literal::kDate: {
            type = std::make_shared<DB::DataTypeDate32>();
            field = literal.date();
            break;
        }
        case substrait::Expression_Literal::kTimestamp: {
            type = std::make_shared<DB::DataTypeDateTime64>(6);
            field = DecimalField<DB::DateTime64>(literal.timestamp(), 6);
            break;
        }
        case substrait::Expression_Literal::kDecimal: {
            UInt32 precision = literal.decimal().precision();
            UInt32 scale = literal.decimal().scale();
            const auto & bytes = literal.decimal().value();

            if (precision <= DB::DataTypeDecimal32::maxPrecision())
            {
                type = std::make_shared<DB::DataTypeDecimal32>(precision, scale);
                auto value = *reinterpret_cast<const Int32 *>(bytes.data());
                field = DecimalField<DB::Decimal32>(value, scale);
            }
            else if (precision <= DataTypeDecimal64::maxPrecision())
            {
                type = std::make_shared<DB::DataTypeDecimal64>(precision, scale);
                auto value = *reinterpret_cast<const Int64 *>(bytes.data());
                field = DecimalField<DB::Decimal64>(value, scale);
            }
            else if (precision <= DataTypeDecimal128::maxPrecision())
            {
                type = std::make_shared<DB::DataTypeDecimal128>(precision, scale);
                String bytes_copy(bytes);
                auto value = *reinterpret_cast<DB::Decimal128 *>(bytes_copy.data());
                field = DecimalField<DB::Decimal128>(value, scale);
            }
            else
                throw DB::Exception(DB::ErrorCodes::UNKNOWN_TYPE, "Spark doesn't support decimal type with precision {}", precision);
            break;
        }
        case substrait::Expression_Literal::kList: {
            const auto & values = literal.list().values();
            if (values.empty())
            {
                type = std::make_shared<DataTypeArray>(std::make_shared<DB::DataTypeNothing>());
                field = Array();
                break;
            }

            DB::DataTypePtr common_type;
            std::tie(common_type, std::ignore) = parse(values[0]);
            size_t list_len = values.size();
            Array array(list_len);
            for (int i = 0; i < static_cast<int>(list_len); ++i)
            {
                auto type_and_field = parse(values[i]);
                common_type = getLeastSupertype(DataTypes{common_type, type_and_field.first});
                array[i] = std::move(type_and_field.second);
            }

            type = std::make_shared<DB::DataTypeArray>(common_type);
            field = std::move(array);
            break;
        }
        case substrait::Expression_Literal::kEmptyList: {
            type = std::make_shared<DB::DataTypeArray>(std::make_shared<DB::DataTypeNothing>());
            field = Array();
            break;
        }
        case substrait::Expression_Literal::kMap: {
            const auto & key_values = literal.map().key_values();
            if (key_values.empty())
            {
                type = std::make_shared<DB::DataTypeMap>(std::make_shared<DB::DataTypeNothing>(), std::make_shared<DB::DataTypeNothing>());
                field = Map();
                break;
            }

            const auto & first_key_value = key_values[0];

            DB::DataTypePtr common_key_type;
            std::tie(common_key_type, std::ignore) = parse(first_key_value.key());

            DB::DataTypePtr common_value_type;
            std::tie(common_value_type, std::ignore) = parse(first_key_value.value());

            Map map;
            map.reserve(key_values.size());
            for (const auto & key_value : key_values)
            {
                Tuple tuple(2);

                DB::DataTypePtr key_type;
                std::tie(key_type, tuple[0]) = parse(key_value.key());
                /// Each key should has the same type
                if (!common_key_type->equals(*key_type))
                    throw DB::Exception(
                        DB::ErrorCodes::LOGICAL_ERROR,
                        "Literal map key type mismatch:{} and {}",
                        common_key_type->getName(),
                        key_type->getName());

                DB::DataTypePtr value_type;
                std::tie(value_type, tuple[1]) = parse(key_value.value());
                /// Each value should has least super type for all of them
                common_value_type = getLeastSupertype(DB::DataTypes{common_value_type, value_type});

                map.emplace_back(std::move(tuple));
            }

            type = std::make_shared<DB::DataTypeMap>(common_key_type, common_value_type);
            field = std::move(map);
            break;
        }
        case substrait::Expression_Literal::kEmptyMap: {
            type = std::make_shared<DB::DataTypeMap>(std::make_shared<DB::DataTypeNothing>(), std::make_shared<DB::DataTypeNothing>());
            field = Map();
            break;
        }
        case substrait::Expression_Literal::kStruct: {
            const auto & fields = literal.struct_().fields();

            DB::DataTypes types;
            types.reserve(fields.size());
            Tuple tuple;
            tuple.reserve(fields.size());
            for (const auto & f : fields)
            {
                DB::DataTypePtr field_type;
                DB::Field field_value;
                std::tie(field_type, field_value) = parse(f);

                types.emplace_back(std::move(field_type));
                tuple.emplace_back(std::move(field_value));
            }

            type = std::make_shared<DB::DataTypeTuple>(types);
            field = std::move(tuple);
            break;
        }
        case substrait::Expression_Literal::kNull: {
            type = TypeParser::parseType(literal.null());
            field = DB::Field{};
            break;
        }
        default: {
            throw DB::Exception(
                DB::ErrorCodes::UNKNOWN_TYPE, "Unsupported spark literal type {}", magic_enum::enum_name(literal.literal_type_case()));
        }
    }
    return std::make_pair(std::move(type), std::move(field));
}

const DB::ActionsDAG::Node *
ExpressionParser::addConstColumn(DB::ActionsDAG & actions_dag, const DB::DataTypePtr type, const DB::Field & field) const
{
    String name = toString(field).substr(0, 10);
    name = getUniqueName(name);
    return &actions_dag.addColumn(DB::ColumnWithTypeAndName(type->createColumnConst(1, field), type, name));
}


const ActionsDAG::Node * ExpressionParser::parseExpression(ActionsDAG & actions_dag, const substrait::Expression & rel) const
{
    switch (rel.rex_type_case())
    {
        case substrait::Expression::RexTypeCase::kLiteral: {
            DB::DataTypePtr type;
            DB::Field field;
            std::tie(type, field) = LiteralParser::parse(rel.literal());
            return addConstColumn(actions_dag, type, field);
        }

        case substrait::Expression::RexTypeCase::kSelection: {
            if (!rel.selection().has_direct_reference() || !rel.selection().direct_reference().has_struct_field())
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Can only have direct struct references in selections");

            const auto * field = actions_dag.getInputs()[rel.selection().direct_reference().struct_field().field()];
            return field;
        }

        case substrait::Expression::RexTypeCase::kCast: {
            if (!rel.cast().has_type() || !rel.cast().has_input())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Doesn't have type or input in cast node.");
            ActionsDAG::NodeRawConstPtrs args;

            const auto & input = rel.cast().input();
            args.emplace_back(parseExpression(actions_dag, input));

            const auto & substrait_type = rel.cast().type();
            const auto & input_type = args[0]->result_type;
            DataTypePtr denull_input_type = removeNullable(input_type);
            DataTypePtr output_type = TypeParser::parseType(substrait_type);
            DataTypePtr denull_output_type = removeNullable(output_type);

            const ActionsDAG::Node * result_node = nullptr;
            if (substrait_type.has_binary())
            {
                /// Spark cast(x as BINARY) -> CH reinterpretAsStringSpark(x)
                result_node = toFunctionNode(actions_dag, "reinterpretAsStringSpark", args);
            }
            else if (isString(denull_input_type) && isDate32(denull_output_type))
                result_node = toFunctionNode(actions_dag, "sparkToDate", args);
            else if (isString(denull_input_type) && isDateTime64(denull_output_type))
                result_node = toFunctionNode(actions_dag, "sparkToDateTime", args);
            else if (isDecimal(denull_input_type) && isString(denull_output_type))
            {
                /// Spark cast(x as STRING) if x is Decimal -> CH toDecimalString(x, scale)
                UInt8 scale = getDecimalScale(*denull_input_type);
                args.emplace_back(addConstColumn(actions_dag, std::make_shared<DataTypeUInt8>(), Field(scale)));
                result_node = toFunctionNode(actions_dag, "toDecimalString", args);
            }
            else if (isFloat(denull_input_type) && isInt(denull_output_type))
            {
                String function_name = "sparkCastFloatTo" + denull_output_type->getName();
                result_node = toFunctionNode(actions_dag, function_name, args);
            }
            else if ((isDecimal(denull_input_type) && substrait_type.has_decimal()))
            {
                args.emplace_back(addConstColumn(actions_dag, std::make_shared<DataTypeInt32>(), substrait_type.decimal().precision()));
                args.emplace_back(addConstColumn(actions_dag, std::make_shared<DataTypeInt32>(), substrait_type.decimal().scale()));
                result_node = toFunctionNode(actions_dag, "checkDecimalOverflowSparkOrNull", args);
            }
            else if (isMap(denull_input_type) && isString(denull_output_type))
            {
                // ISSUE-7389: spark cast(map to string) has different behavior with CH cast(map to string)
                auto map_input_type = std::static_pointer_cast<const DataTypeMap>(denull_input_type);
                args.emplace_back(addConstColumn(actions_dag, map_input_type->getKeyType(), map_input_type->getKeyType()->getDefault()));
                args.emplace_back(
                    addConstColumn(actions_dag, map_input_type->getValueType(), map_input_type->getValueType()->getDefault()));
                result_node = toFunctionNode(actions_dag, "sparkCastMapToString", args);
            }
            else if (isString(denull_input_type) && substrait_type.has_bool_())
            {
                /// cast(string to boolean)
                args.emplace_back(addConstColumn(actions_dag, std::make_shared<DataTypeString>(), output_type->getName()));
                result_node = toFunctionNode(actions_dag, "accurateCastOrNull", args);
            }
            else if (isString(denull_input_type) && isInt(denull_output_type))
            {
                /// Spark cast(x as INT) if x is String -> CH cast(trim(x) as INT)
                /// Refer to https://github.com/apache/incubator-gluten/issues/4956
                args[0] = toFunctionNode(actions_dag, "trim", {args[0]});
                args.emplace_back(addConstColumn(actions_dag, std::make_shared<DataTypeString>(), output_type->getName()));
                result_node = toFunctionNode(actions_dag, "CAST", args);
            }
            else
            {
                /// Common process: CAST(input, type)
                args.emplace_back(addConstColumn(actions_dag, std::make_shared<DataTypeString>(), output_type->getName()));
                result_node = toFunctionNode(actions_dag, "CAST", args);
            }

            actions_dag.addOrReplaceInOutputs(*result_node);
            return result_node;
        }

        case substrait::Expression::RexTypeCase::kIfThen: {
            const auto & if_then = rel.if_then();
            DB::FunctionOverloadResolverPtr function_ptr = nullptr;
            auto condition_nums = if_then.ifs_size();
            if (condition_nums == 1)
                function_ptr = DB::FunctionFactory::instance().get("if", context->queryContext());
            else
                function_ptr = FunctionFactory::instance().get("multiIf", context->queryContext());
            DB::ActionsDAG::NodeRawConstPtrs args;

            for (int i = 0; i < condition_nums; ++i)
            {
                const auto & ifs = if_then.ifs(i);
                const auto * if_node = parseExpression(actions_dag, ifs.if_());
                args.emplace_back(if_node);

                const auto * then_node = parseExpression(actions_dag, ifs.then());
                args.emplace_back(then_node);
            }

            const auto * else_node = parseExpression(actions_dag, if_then.else_());
            args.emplace_back(else_node);
            std::string args_name = join(args, ',');
            std::string result_name;
            if (condition_nums == 1)
                result_name = "if(" + args_name + ")";
            else
                result_name = "multiIf(" + args_name + ")";
            const auto * function_node = &actions_dag.addFunction(function_ptr, args, result_name);
            actions_dag.addOrReplaceInOutputs(*function_node);
            return function_node;
        }

        case substrait::Expression::RexTypeCase::kScalarFunction: {
            return parseFunction(rel.scalar_function(), actions_dag);
        }

        case substrait::Expression::RexTypeCase::kSingularOrList: {
            const auto & options = rel.singular_or_list().options();
            /// options is empty always return false
            if (options.empty())
                return addConstColumn(actions_dag, std::make_shared<DB::DataTypeUInt8>(), 0);
            /// options should be literals
            if (!options[0].has_literal())
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Options of SingularOrList must have literal type");

            DB::ActionsDAG::NodeRawConstPtrs args;
            args.emplace_back(parseExpression(actions_dag, rel.singular_or_list().value()));

            bool nullable = false;
            int options_len = options.size();
            for (int i = 0; i < options_len; ++i)
            {
                if (!options[i].has_literal())
                    throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "in expression values must be the literal!");
                if (!nullable)
                    nullable = options[i].literal().has_null();
            }

            DB::DataTypePtr elem_type;
            std::tie(elem_type, std::ignore) = LiteralParser::parse(options[0].literal());
            elem_type = wrapNullableType(nullable, elem_type);

            DB::MutableColumnPtr elem_column = elem_type->createColumn();
            elem_column->reserve(options_len);
            for (int i = 0; i < options_len; ++i)
            {
                auto type_and_field = LiteralParser::parse(options[i].literal());
                auto option_type = wrapNullableType(nullable, type_and_field.first);
                if (!elem_type->equals(*option_type))
                    throw DB::Exception(
                        DB::ErrorCodes::LOGICAL_ERROR,
                        "SingularOrList options type mismatch:{} and {}",
                        elem_type->getName(),
                        option_type->getName());

                elem_column->insert(type_and_field.second);
            }
            auto name = getUniqueName("__set");
            ColumnWithTypeAndName elem_block{std::move(elem_column), elem_type, name};

            PreparedSets prepared_sets;
            FutureSet::Hash emptyKey;
            auto future_set = prepared_sets.addFromTuple(emptyKey, {elem_block}, context->queryContext()->getSettingsRef());
            auto arg = DB::ColumnSet::create(1, std::move(future_set));
            args.emplace_back(&actions_dag.addColumn(DB::ColumnWithTypeAndName(std::move(arg), std::make_shared<DB::DataTypeSet>(), name)));

            const auto * function_node = toFunctionNode(actions_dag, "in", args);
            actions_dag.addOrReplaceInOutputs(*function_node);
            if (nullable)
            {
                /// if sets has `null` and value not in sets
                /// In Spark: return `null`, is the standard behaviour from ANSI.(SPARK-37920)
                /// In CH: return `false`
                /// So we used if(a, b, c) cast `false` to `null` if sets has `null`
                auto type = wrapNullableType(true, function_node->result_type);
                DB::ActionsDAG::NodeRawConstPtrs cast_args(
                    {function_node, addConstColumn(actions_dag, type, true), addConstColumn(actions_dag, type, DB::Field())});
                auto cast = DB::FunctionFactory::instance().get("if", context->queryContext());
                function_node = toFunctionNode(actions_dag, "if", cast_args);
                actions_dag.addOrReplaceInOutputs(*function_node);
            }
            return function_node;
        }

        default:
            throw DB::Exception(
                DB::ErrorCodes::UNKNOWN_TYPE,
                "Unsupported spark expression type {} : {}",
                magic_enum::enum_name(rel.rex_type_case()),
                rel.DebugString());
    }
}

DB::ActionsDAG
ExpressionParser::expressionsToActionsDAG(const std::vector<substrait::Expression> & expressions, const DB::Block & header) const
{
    DB::ActionsDAG actions_dag(header.getNamesAndTypesList());
    DB::NamesWithAliases required_columns;
    std::set<String> distinct_columns;

    for (const auto & expr : expressions)
    {
        if (expr.has_selection())
        {
            auto position = expr.selection().direct_reference().struct_field().field();
            auto col_name = header.getByPosition(position).name;
            const DB::ActionsDAG::Node * field = actions_dag.tryFindInOutputs(col_name);
            if (!field)
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Not found {} in actions dag's output", col_name);
            if (distinct_columns.contains(field->result_name))
            {
                auto unique_name = getUniqueName(field->result_name);
                required_columns.emplace_back(DB::NameWithAlias(field->result_name, unique_name));
                distinct_columns.emplace(unique_name);
            }
            else
            {
                required_columns.emplace_back(DB::NameWithAlias(field->result_name, field->result_name));
                distinct_columns.emplace(field->result_name);
            }
        }
        else if (expr.has_scalar_function())
        {
            const auto & scalar_function = expr.scalar_function();
            auto signature_name = getFunctionNameInSignature(scalar_function);

            std::vector<String> result_names;
            if (signature_name == "explode")
            {
                auto result_nodes = parseArrayJoin(scalar_function, actions_dag, false);
                for (const auto * node : result_nodes)
                    result_names.emplace_back(node->result_name);
            }
            else if (signature_name == "posexplode")
            {
                auto result_nodes = parseArrayJoin(scalar_function, actions_dag, true);
                for (const auto * node : result_nodes)
                    result_names.emplace_back(node->result_name);
            }
            else if (signature_name == "json_tuple")
            {
                auto result_nodes = parseJsonTuple(scalar_function, actions_dag);
                for (const auto * node : result_nodes)
                    result_names.emplace_back(node->result_name);
            }
            else
            {
                result_names.resize(1);
                result_names[0] = parseFunction(scalar_function, actions_dag, true)->result_name;
            }

            for (const auto & result_name : result_names)
            {
                if (result_name.empty())
                    continue;

                if (distinct_columns.contains(result_name))
                {
                    auto unique_name = getUniqueName(result_name);
                    required_columns.emplace_back(NameWithAlias(result_name, unique_name));
                    distinct_columns.emplace(unique_name);
                }
                else
                {
                    required_columns.emplace_back(NameWithAlias(result_name, result_name));
                    distinct_columns.emplace(result_name);
                }
            }
        }
        else if (expr.has_cast() || expr.has_if_then() || expr.has_literal() || expr.has_singular_or_list())
        {
            const auto * node = parseExpression(actions_dag, expr);
            actions_dag.addOrReplaceInOutputs(*node);
            if (distinct_columns.contains(node->result_name))
            {
                auto unique_name = getUniqueName(node->result_name);
                required_columns.emplace_back(NameWithAlias(node->result_name, unique_name));
                distinct_columns.emplace(unique_name);
            }
            else
            {
                required_columns.emplace_back(NameWithAlias(node->result_name, node->result_name));
                distinct_columns.emplace(node->result_name);
            }
        }
        else
            throw DB::Exception(
                DB::ErrorCodes::BAD_ARGUMENTS, "unsupported projection type {}.", magic_enum::enum_name(expr.rex_type_case()));
    }
    actions_dag.project(required_columns);
    actions_dag.appendInputsForUnusedColumns(header);
    return actions_dag;
}

DB::ActionsDAG::NodeRawConstPtrs
ExpressionParser::parseFunctionArguments(DB::ActionsDAG & actions_dag, const substrait::Expression_ScalarFunction & func) const
{
    DB::ActionsDAG::NodeRawConstPtrs parsed_args;
    parsed_args.reserve(func.arguments_size());
    for (Int32 i = 0; i < func.arguments_size(); ++i)
    {
        const auto & arg = func.arguments(i);
        if (!arg.has_value())
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unknow scalar function:{}\n\n{}", func.DebugString(), arg.DebugString());
        const auto * node = parseExpression(actions_dag, arg.value());
        parsed_args.emplace_back(node);
    }
    return parsed_args;
}

const DB::ActionsDAG::Node *
ExpressionParser::parseFunction(const substrait::Expression_ScalarFunction & func, DB::ActionsDAG & actions_dag, bool add_to_output) const
{
    auto function_signature = getFunctionNameInSignature(func);
    auto function_parser = FunctionParserFactory::instance().get(function_signature, context);
    const auto * function_node = function_parser->parse(func, actions_dag);
    if (add_to_output)
        actions_dag.addOrReplaceInOutputs(*function_node);
    return function_node;
}

const DB::ActionsDAG::Node * ExpressionParser::toFunctionNode(
    DB::ActionsDAG & actions_dag,
    const String & ch_function_name,
    const DB::ActionsDAG::NodeRawConstPtrs & args,
    const String & result_name_) const
{
    auto function_builder = FunctionFactory::instance().get(ch_function_name, context->queryContext());
    std::string result_name = result_name_;
    if (result_name.empty())
    {
        std::string args_name = join(args, ',');
        result_name = ch_function_name + "(" + args_name + ")";
    }
    return &actions_dag.addFunction(function_builder, args, result_name);
}

std::atomic<UInt64> ExpressionParser::unique_name_counter = 0;
String ExpressionParser::getUniqueName(const String & name) const
{
    return name + "_" + std::to_string(unique_name_counter++);
}

String ExpressionParser::getFunctionNameInSignature(const substrait::Expression_ScalarFunction & func_) const
{
    return getFunctionNameInSignature(func_.function_reference());
}

String ExpressionParser::getFunctionNameInSignature(UInt32 func_ref_) const
{
    auto function_sig = context->getFunctionNameInSignature(func_ref_);
    if (!function_sig)
        throw DB::Exception(DB::ErrorCodes::UNKNOWN_FUNCTION, "Unknown function anchor: {}", func_ref_);
    return *function_sig;
}

String ExpressionParser::getFunctionName(const substrait::Expression_ScalarFunction & func_) const
{
    auto signature_name = getFunctionNameInSignature(func_);
    auto function_parser = FunctionParserFactory::instance().tryGet(signature_name, context);
    if (!function_parser)
        throw DB::Exception(DB::ErrorCodes::UNKNOWN_FUNCTION, "Unsupported function {}", signature_name);
    return function_parser->getCHFunctionName(func_);
}

String ExpressionParser::safeGetFunctionName(const substrait::Expression_ScalarFunction & func_) const
{
    try
    {
        return getFunctionName(func_);
    }
    catch (const DB::Exception &)
    {
        return "";
    }
}


DB::ActionsDAG::NodeRawConstPtrs ExpressionParser::parseArrayJoinArguments(
    const substrait::Expression_ScalarFunction & func, DB::ActionsDAG & actions_dag, bool position, bool & is_map) const
{
    auto parsed_args = parseFunctionArguments(actions_dag, func);

    const auto arg0_type = DB::removeNullable(parsed_args[0]->result_type);
    if (isMap(arg0_type))
        is_map = true;
    else if (isArray(arg0_type))
        is_map = false;
    else
        throw DB::Exception(
            DB::ErrorCodes::BAD_ARGUMENTS, "Argument type of arrayJoin should be Array or Map but is {}", arg0_type->getName());

    /// Remove Nullable for input argument of arrayJoin function because arrayJoin function only accept non-nullable input
    /// array() or map()
    const auto * empty_node = addConstColumn(actions_dag, arg0_type, is_map ? DB::Field(Map()) : DB::Field(Array()));
    /// ifNull(arg, array()) or ifNull(arg, map())
    const auto * if_null_node = toFunctionNode(actions_dag, "ifNull", {parsed_args[0], empty_node});
    /// assumeNotNull(ifNull(arg, array())) or assumeNotNull(ifNull(arg, map()))
    const auto * not_null_node = toFunctionNode(actions_dag, "assumeNotNull", {if_null_node});
    /// Wrap with materalize function to make sure column input to ARRAY JOIN STEP is materaized
    const auto * arg = &actions_dag.materializeNode(*not_null_node);

    /// If spark function is posexplode, we need to add position column together with input argument
    if (position)
    {
        /// length(arg)
        const auto * length_node = toFunctionNode(actions_dag, "length", {arg});
        /// range(length(arg))
        const auto * range_node = toFunctionNode(actions_dag, "range", {length_node});
        /// mapFromArrays(range(length(arg)), arg)
        arg = toFunctionNode(actions_dag, "mapFromArrays", {range_node, arg});
    }
    parsed_args[0] = arg;
    return parsed_args;
}

DB::ActionsDAG::NodeRawConstPtrs
ExpressionParser::parseArrayJoin(const substrait::Expression_ScalarFunction & func, DB::ActionsDAG & actions_dag, bool position) const
{
    /// Whether the input argument of explode/posexplode is map type
    bool is_map = false;
    auto parsed_args = parseArrayJoinArguments(func, actions_dag, position, is_map);

    /// Note: Make sure result_name keep the same after applying arrayJoin function, which makes it much easier to transform arrayJoin function to ARRAY JOIN STEP
    /// Otherwise an alias node must be appended after ARRAY JOIN STEP, which is not a graceful implementation.
    const auto & arg_not_null = parsed_args[0];
    auto array_join_name = arg_not_null->result_name;
    /// arrayJoin(arg_not_null)
    const auto * array_join_node = &actions_dag.addArrayJoin(*arg_not_null, array_join_name);

    auto tuple_element_builder = FunctionFactory::instance().get("sparkTupleElement", context->queryContext());
    auto tuple_index_type = std::make_shared<DB::DataTypeUInt32>();
    auto add_tuple_element = [&](const DB::ActionsDAG::Node * tuple_node, size_t i) -> const ActionsDAG::Node *
    {
        DB::ColumnWithTypeAndName index_col(tuple_index_type->createColumnConst(1, i), tuple_index_type, getUniqueName(std::to_string(i)));
        const auto * index_node = &actions_dag.addColumn(std::move(index_col));
        auto result_name = "sparkTupleElement(" + tuple_node->result_name + ", " + index_node->result_name + ")";
        return &actions_dag.addFunction(tuple_element_builder, {tuple_node, index_node}, result_name);
    };

    /// Special process to keep compatiable with Spark
    if (!position)
    {
        /// Spark: explode(array_or_map) -> CH: arrayJoin(array_or_map)
        if (is_map)
        {
            /// In Spark: explode(map(k, v)) output 2 columns with default names "key" and "value"
            /// In CH: arrayJoin(map(k, v)) output 1 column with Tuple Type.
            /// So we must wrap arrayJoin with sparkTupleElement function for compatiability.

            /// arrayJoin(arg_not_null).1
            const auto * key_node = add_tuple_element(array_join_node, 1);
            /// arrayJoin(arg_not_null).2
            const auto * val_node = add_tuple_element(array_join_node, 2);

            actions_dag.addOrReplaceInOutputs(*key_node);
            actions_dag.addOrReplaceInOutputs(*val_node);
            return {key_node, val_node};
        }
        else
        {
            actions_dag.addOrReplaceInOutputs(*array_join_node);
            return {array_join_node};
        }
    }
    else
    {
        /// Spark: posexplode(array_or_map) -> CH: arrayJoin(map), in which map = mapFromArrays(range(length(array_or_map)), array_or_map)

        /// In Spark: posexplode(array_of_map) output 2 or 3 columns: (pos, col) or (pos, key, value)
        /// In CH: arrayJoin(map(k, v)) output 1 column with Tuple Type.
        /// So we must wrap arrayJoin with sparkTupleElement function for compatiability.

        /// pos = cast(arrayJoin(arg_not_null).1, "Int32")
        const auto * pos_node = add_tuple_element(array_join_node, 1);
        pos_node = ActionsDAGUtil::convertNodeType(actions_dag, pos_node, INT());

        /// if is_map is false, output col = arrayJoin(arg_not_null).2
        /// if is_map is true,  output (key, value) = arrayJoin(arg_not_null).2
        const auto * item_node = add_tuple_element(array_join_node, 2);

        if (is_map)
        {
            /// key = arrayJoin(arg_not_null).2.1
            const auto * key_node = add_tuple_element(item_node, 1);

            /// value = arrayJoin(arg_not_null).2.2
            const auto val_node = add_tuple_element(item_node, 2);

            actions_dag.addOrReplaceInOutputs(*pos_node);
            actions_dag.addOrReplaceInOutputs(*key_node);
            actions_dag.addOrReplaceInOutputs(*val_node);
            return {pos_node, key_node, val_node};
        }
        else
        {
            actions_dag.addOrReplaceInOutputs(*pos_node);
            actions_dag.addOrReplaceInOutputs(*item_node);
            return {pos_node, item_node};
        }
    }
}

DB::ActionsDAG::NodeRawConstPtrs
ExpressionParser::parseJsonTuple(const substrait::Expression_ScalarFunction & func, DB::ActionsDAG & actions_dag) const
{
    const auto & pb_args = func.arguments();
    if (pb_args.size() < 2)
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "json_tuple function has at least 2 arguments");

    const auto & first_arg = pb_args[0].value();
    const auto * json_expr_node = parseExpression(actions_dag, first_arg);
    DB::WriteBufferFromOwnString write_buffer;
    write_buffer << "Tuple(";
    for (int i = 1; i < pb_args.size(); ++i)
    {
        if (i > 1)
            write_buffer << ", ";
        const auto & arg = pb_args[i].value();
        if (!arg.has_literal() || !arg.literal().has_string())
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "json_tuple function requires string literal arguments");

        write_buffer << arg.literal().string() << " Nullable(String)";
    }
    write_buffer << ")";
    const auto * extract_expr_node = addConstColumn(actions_dag, std::make_shared<DB::DataTypeString>(), write_buffer.str());
    auto json_extract_builder = DB::FunctionFactory::instance().get("JSONExtract", context->queryContext());
    auto json_extract_result_name = "JSONExtract(" + json_expr_node->result_name + ", " + extract_expr_node->result_name + ")";
    const auto json_extract_node
        = &actions_dag.addFunction(json_extract_builder, {json_expr_node, extract_expr_node}, json_extract_result_name);
    auto tuple_element_builder = DB::FunctionFactory::instance().get("sparkTupleElement", context->queryContext());
    auto tuple_index_type = std::make_shared<DB::DataTypeUInt32>();
    auto add_tuple_element = [&](const DB::ActionsDAG::Node * tuple_node, size_t i) -> const ActionsDAG::Node *
    {
        DB::ColumnWithTypeAndName index_col(tuple_index_type->createColumnConst(1, i), tuple_index_type, getUniqueName(std::to_string(i)));
        const auto * index_node = &actions_dag.addColumn(std::move(index_col));
        auto result_name = "sparkTupleElement(" + tuple_node->result_name + ", " + index_node->result_name + ")";
        return &actions_dag.addFunction(tuple_element_builder, {tuple_node, index_node}, result_name);
    };

    DB::ActionsDAG::NodeRawConstPtrs res_nodes;
    for (int i = 1; i < pb_args.size(); ++i)
    {
        const auto * tuple_node = add_tuple_element(json_extract_node, i);
        actions_dag.addOrReplaceInOutputs(*tuple_node);
        res_nodes.push_back(tuple_node);
    }
    return res_nodes;
}
}
