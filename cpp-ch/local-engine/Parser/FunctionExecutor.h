#pragma once

#include <Core/Block.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Parser/SerializedPlanParser.h>
#include <base/types.h>
#include <substrait/algebra.pb.h>
#include <substrait/extensions/extensions.pb.h>

namespace local_engine
{


class FunctionExecutor
{
public:
    struct TestCase
    {
        std::vector<DB::Field> inputs;
        DB::Field expect_output;
    };

    FunctionExecutor(
        const String & name_, const DB::DataTypes & input_types_, const DB::DataTypePtr & output_type_, const DB::ContextPtr & context_)
        : name(name_)
        , input_types(input_types_)
        , output_type(output_type_)
        , plan_parser(context_)
        , log(&Poco::Logger::get("FunctionExecutor"))
    {
        buildExtensions();
        buildExpression();
        buildHeader();

        parseExtensions();
        parseExpression();
    }

    void execute(DB::Block & block);

    bool executeAndCompare(const std::vector<TestCase> & cases);

    Block getHeader() const;

    String getResultName() const;

private:
    void buildExtensions();
    void buildExpression();
    void buildHeader();

    void parseExtensions();
    void parseExpression();

    /// substrait scalar function name
    String name;
    DB::DataTypes input_types;
    DB::DataTypePtr output_type;
    SerializedPlanParser plan_parser;

    ::google::protobuf::RepeatedPtrField<substrait::extensions::SimpleExtensionDeclaration> extensions;
    substrait::Expression expression;
    DB::Block header;
    String result_name;
    std::unique_ptr<ExpressionActions> expression_actions;

    Poco::Logger * log;
};

}
