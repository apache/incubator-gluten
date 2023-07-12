#include <Parser/FunctionParser.h>
#include <Common/CHUtil.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
}

namespace local_engine
{

class FunctionParserMurmur3Hash : public FunctionParser
{
public:
    explicit FunctionParserMurmur3Hash(SerializedPlanParser * plan_parser_) : FunctionParser(plan_parser_) { }

    static constexpr auto name = "murmur3hash";

    String getName() const override { return name; }

    const ActionsDAG::Node * parse(
        const substrait::Expression_ScalarFunction & substrait_func,
        ActionsDAGPtr & actions_dag) const override
    {
        auto parsed_args = parseFunctionArguments(substrait_func, "", actions_dag);

        /// Cast all arguments with Int8/Int16 to Int32
        for (auto & arg : parsed_args)
        {
            auto arg_type = arg->result_type;

            WhichDataType which(removeNullable(arg_type));
            bool is_nullable = arg_type->isNullable();
            if (which.isInt8() || which.isInt16())
            {
                String int32_type = is_nullable ? "Nullable(Int32)" : "Int32";
                const auto * int32_type_node = addColumnToActionsDAG(actions_dag, std::make_shared<DataTypeString>(), int32_type);
                arg = toFunctionNode(actions_dag, "CAST", {arg, int32_type_node});
            }
            else
                continue;
        }

        const auto * hash_node = toFunctionNode(actions_dag, "murmurHashSpark3_32", parsed_args);
        return convertNodeTypeIfNeeded(substrait_func, hash_node, actions_dag);
    }
};

static FunctionParserRegister<FunctionParserMurmur3Hash> register_murmur3hash;
}
