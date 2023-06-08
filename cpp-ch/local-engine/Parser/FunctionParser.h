#pragma once

#include <substrait/algebra.pb.h>
#include <boost/noncopyable.hpp>

#include <base/types.h>
#include <Common/IFactoryWithAliases.h>
#include <Interpreters/ActionsDAG.h>
#include <Parser/SerializedPlanParser.h>

namespace local_engine
{
class SerializedPlanParser;

/// Parse a single substrait scalar function
class FunctionParser
{
public:
    explicit FunctionParser(SerializedPlanParser * plan_parser_) : plan_parser(plan_parser_) { }

    virtual ~FunctionParser() = default;

    virtual String getName() const = 0;

    /// Input: substrait_func, action_dag
    /// Output: actions_dag, required_columns, and result_node
    virtual const DB::ActionsDAG::Node * parse(
        const substrait::Expression_ScalarFunction & substrait_func,
        DB::ActionsDAGPtr & actions_dag,
        std::vector<String> & required_columns) const;

protected:
    virtual String getCHFunctionName(const substrait::Expression_ScalarFunction & substrait_func) const;

    virtual DB::ActionsDAG::NodeRawConstPtrs parseFunctionArguments(
        const substrait::Expression_ScalarFunction & substrait_func,
        const String & ch_func_name,
        DB::ActionsDAGPtr & actions_dag,
        std::vector<String> & required_columns) const;

    virtual const DB::ActionsDAG::Node * convertNodeTypeIfNeeded(
        const substrait::Expression_ScalarFunction & substrait_func,
        const DB::ActionsDAG::Node * func_node,
        DB::ActionsDAGPtr & actions_dag) const;

    DB::ContextPtr getContext() const { return plan_parser->context; }

    String getUniqueName(const String & name) const { return plan_parser->getUniqueName(name); }

    const DB::ActionsDAG::Node * addColumnToActionsDAG(DB::ActionsDAGPtr & actions_dag, const DB::DataTypePtr & type, const DB::Field & field) const
    {
        return &actions_dag->addColumn(ColumnWithTypeAndName(type->createColumnConst(1, field), type, getUniqueName(toString(field))));
    }

    const DB::ActionsDAG::Node *
    toFunctionNode(DB::ActionsDAGPtr & action_dag, const String & func_name, const DB::ActionsDAG::NodeRawConstPtrs & args) const
    {
        return plan_parser->toFunctionNode(action_dag, func_name, args);
    }

    SerializedPlanParser * plan_parser;
};

using FunctionParserPtr = std::shared_ptr<FunctionParser>;
using FunctionParserCreator = std::function<FunctionParserPtr(SerializedPlanParser *)>;

/// Creates FunctionParser by name.
class FunctionParserFactory : private boost::noncopyable, public DB::IFactoryWithAliases<FunctionParserCreator>
{
public:
    using Parsers = std::unordered_map<std::string, Value>;

    static FunctionParserFactory & instance();

    /// Register a function parser by its name.
    /// No locking, you must register all functions before usage of get.
    void registerFunctionParser(const String & name, Value value);

    template <typename Parser>
    void registerFunctionParser()
    {
        // std::cout << "register function parser with name:" << Parser::name << std::endl;
        auto creator
            = [](SerializedPlanParser * plan_parser) -> std::shared_ptr<FunctionParser> { return std::make_shared<Parser>(plan_parser); };

        registerFunctionParser(Parser::name, creator);
    }

    FunctionParserPtr get(const String & name, SerializedPlanParser * plan_parser);
    FunctionParserPtr tryGet(const String & name, SerializedPlanParser * plan_parser);
    const Parsers & getMap() const override {return parsers;}

private:

    Parsers parsers;

    /// Always empty
    Parsers case_insensitive_parsers;


    const Parsers & getCaseInsensitiveMap() const override { return case_insensitive_parsers; }

    String getFactoryName() const override { return "FunctionParserFactory"; }
};

template <typename Parser>
struct FunctionParserRegister
{
    FunctionParserRegister() { FunctionParserFactory::instance().registerFunctionParser<Parser>(); }
};

}
