#include <Parser/FunctionParser.h>
#include <DataTypes/IDataType.h>
#include <Common/CHUtil.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/FunctionHelpers.h>

namespace local_engine
{

class FunctionParserArrayContains : public FunctionParser
{
public:
    explicit FunctionParserArrayContains(SerializedPlanParser * plan_parser_) : FunctionParser(plan_parser_) { }
    ~FunctionParserArrayContains() override = default;

    static constexpr auto name = "array_contains";

    String getName() const override { return name; }

    String getCHFunctionName(const CommonFunctionInfo &) const override { return "has"; }

    String getCHFunctionName(const DB::DataTypes &) const override { return "has"; }

    const ActionsDAG::Node * parse(
    const substrait::Expression_ScalarFunction & substrait_func,
    ActionsDAGPtr & actions_dag) const override
    {
        /**
            parse array_contains(arr, value) as
            if (isNull(arr) || isNull(value))
                null
            else
                if (has(assertNotNull(arr), value))
                    true
                else if (has(assertNotNull(arr), null))
                    null
                else
                    false

            result nullable:
                arr.nullable || value.nullable || arr.dataType.asInstanceOf[ArrayType].containsNull
        */

        auto func_info = CommonFunctionInfo{substrait_func};
        auto parsed_args = parseFunctionArguments(func_info, "", actions_dag);
        if (parsed_args.size() != 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {} requires exactly two arguments", getName());

        auto ch_function_name = getCHFunctionName(func_info);

        const auto * arr_arg = parsed_args[0];
        const auto * value_arg = parsed_args[1];

        const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(DB::removeNullable(arr_arg->result_type).get());
        if (!array_type)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "First argument for function {} must be an array", getName());

        auto is_arr_nullable = arr_arg->result_type->isNullable();
        auto is_value_nullable = value_arg->result_type->isNullable();
        auto is_arr_elem_nullable = array_type->getNestedType()->isNullable();

        if (!is_arr_nullable && !is_value_nullable && !is_arr_elem_nullable)
        {
            return toFunctionNode(actions_dag, ch_function_name, {arr_arg, value_arg});
        }

        // has(assertNotNull(arr), value)
        const auto * arr_not_null_node = is_arr_nullable ? toFunctionNode(actions_dag, "assumeNotNull", {arr_arg}) : arr_arg;
        const auto * has_arr_value_node = toFunctionNode(actions_dag, ch_function_name, {arr_not_null_node, value_arg});

        // has(assertNotNull(arr), null)
        const auto * arr_element_null_const_node = addColumnToActionsDAG(actions_dag, array_type->getNestedType(), Field{});
        const auto * has_arr_null_node = toFunctionNode(actions_dag, ch_function_name, {arr_not_null_node, arr_element_null_const_node});

        // should return nullable result
        DataTypePtr wrap_arr_nullable_type = wrapNullableType(true, has_arr_value_node->result_type);
        DataTypePtr wrap_boolean_nullable_type = wrapNullableType(true, std::make_shared<DataTypeUInt8>());
        const auto * null_const_node = addColumnToActionsDAG(actions_dag, wrap_arr_nullable_type, Field{});
        const auto * true_node = addColumnToActionsDAG(actions_dag, wrap_boolean_nullable_type, 1);
        const auto * false_node = addColumnToActionsDAG(actions_dag, wrap_boolean_nullable_type, 0);

        const auto * arr_is_null_node = toFunctionNode(actions_dag, "isNull", {arr_arg});
        const auto * value_is_null_node = toFunctionNode(actions_dag, "isNull", {value_arg});
        const auto * or_node = toFunctionNode(actions_dag, "or", {arr_is_null_node, value_is_null_node});

        const auto * nested_multi_if_node = toFunctionNode(actions_dag, "multiIf", {
            has_arr_value_node,
            true_node,
            has_arr_null_node,
            null_const_node,
            false_node
        });

        return toFunctionNode(actions_dag, "if", {or_node, null_const_node, nested_multi_if_node});
    }
};

static FunctionParserRegister<FunctionParserArrayContains> register_array_contains;
}
