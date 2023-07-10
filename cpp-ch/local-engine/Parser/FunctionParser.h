#pragma once

#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Parser/SerializedPlanParser.h>
#include <base/types.h>
#include <boost/noncopyable.hpp>
#include <substrait/algebra.pb.h>
#include <Common/IFactoryWithAliases.h>

namespace local_engine
{
class SerializedPlanParser;

/// Parse a single substrait scalar function
class FunctionParser
{
public:
    /// Manage scalar, aggregate and window functions by FunctionParser.
    /// Usally we need to make pre-projections and a post-projection for functions.

    /// CommonFunctionInfo is commmon representation for different function types, 
    struct CommonFunctionInfo
    {
        /// basic common function informations
        using Arguments = google::protobuf::RepeatedPtrField<substrait::FunctionArgument>;
        using SortFields = google::protobuf::RepeatedPtrField<substrait::SortField>;
        DB::Int32 function_ref;
        Arguments arguments;
        substrait::Type output_type;
    
        /// Following is for aggregate and window functions.
        substrait::AggregationPhase phase;
        SortFields sort_fields;
        // only be used in aggregate functions at present.
        substrait::Expression filter;
        bool is_in_window = false;
        bool is_aggregate_function = false;
        bool has_filter = false;

        CommonFunctionInfo()
        {
            function_ref = -1;
        }

        CommonFunctionInfo(const substrait::Expression_ScalarFunction & substrait_func)
            : function_ref(substrait_func.function_reference())
            , arguments(substrait_func.arguments())
            , output_type(substrait_func.output_type())
        {
        }

        CommonFunctionInfo(const substrait::WindowRel::Measure & win_measure)
            : function_ref(win_measure.measure().function_reference())
            , arguments(win_measure.measure().arguments())
            , output_type(win_measure.measure().output_type())
            , phase(win_measure.measure().phase())
            , sort_fields(win_measure.measure().sorts())
        {
            is_in_window = true;
            is_aggregate_function = true;
        }

        CommonFunctionInfo(const substrait::AggregateRel::Measure & agg_measure)
            : function_ref(agg_measure.measure().function_reference())
            , arguments(agg_measure.measure().arguments())
            , output_type(agg_measure.measure().output_type())
            , phase(agg_measure.measure().phase())
            , sort_fields(agg_measure.measure().sorts())
            , filter(agg_measure.filter())
        {
            has_filter = agg_measure.has_filter();
            is_aggregate_function = true;
        }
    };
    explicit FunctionParser(SerializedPlanParser * plan_parser_) : plan_parser(plan_parser_) { }

    virtual ~FunctionParser() = default;

    virtual String getName() const = 0;

    /// This is a convenient interface for scalar functions, and should not be used for aggregate functions.
    /// It usally take following actions:
    /// - add const columns for literal arguments into actions_dag.
    /// - make pre-projections for input arguments. e.g. type conversion.
    /// - make a post-projection for the function result. e.g. type conversion.
    virtual const DB::ActionsDAG::Node * parse(
        const substrait::Expression_ScalarFunction & substrait_func,
        DB::ActionsDAGPtr & actions_dag) const;
    /// We recomment to implement this in the subclass instead of the previous one.
    /// The previous one is mainly for backward compatibility.
    virtual const DB::ActionsDAG::Node * parse(
        const CommonFunctionInfo & func_info,
        DB::ActionsDAGPtr & actions_dag) const;

    /// In some special cases, different arguments size or different arguments types may refer to different
    /// CH function implementation.
    virtual String getCHFunctionName(const CommonFunctionInfo & func_info) const;
    /// In most cases, arguments size and types are enough to determine the CH function implementation.
    /// This is only be used in SerializedPlanParser::parseNameStructure.
    virtual String getCHFunctionName(const DB::DataTypes & args) const;

    /// Do some preprojections for the function arguments, and return the necessary arguments for the CH function.
    virtual DB::ActionsDAG::NodeRawConstPtrs parseFunctionArguments(
        const CommonFunctionInfo & func_info,
        const String & ch_func_name,
        DB::ActionsDAGPtr & actions_dag) const;
    DB::ActionsDAG::NodeRawConstPtrs parseFunctionArguments(
        const CommonFunctionInfo & func_info,
        DB::ActionsDAGPtr & actions_dag) const;

    /// Make a postprojection for the function result.
    virtual const DB::ActionsDAG::Node * convertNodeTypeIfNeeded(
        const CommonFunctionInfo & func_info,
        const DB::ActionsDAG::Node * func_node,
        DB::ActionsDAGPtr & actions_dag) const;

    /// Parameters are only used in aggregate functions at present. e.g. percentiles(0.5)(x).
    /// 0.5 is the parameter of percentiles function.
    virtual DB::Array parseFunctionParameters(const CommonFunctionInfo & /*func_info*/) const
    {
        return DB::Array();
    }
    /// Return the default parameters of the function. It's useful for creating a default function instance.
    virtual DB::Array getDefaultFunctionParameters() const
    {
        return DB::Array();
    }

protected:
    virtual String getCHFunctionName(const substrait::Expression_ScalarFunction & substrait_func) const;

    virtual DB::ActionsDAG::NodeRawConstPtrs parseFunctionArguments(
        const substrait::Expression_ScalarFunction & substrait_func,
        const String & ch_func_name,
        DB::ActionsDAGPtr & actions_dag) const;

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
    
    const DB::ActionsDAG::Node *
    toFunctionNode(DB::ActionsDAGPtr & action_dag, const String & func_name, const String & result_name, const DB::ActionsDAG::NodeRawConstPtrs & args) const
    {
        auto function_builder = DB::FunctionFactory::instance().get(func_name, getContext());
        return &action_dag->addFunction(function_builder, args, result_name);
    }

    const DB::ActionsDAG::Node * parseExpression(DB::ActionsDAGPtr actions_dag, const substrait::Expression & rel) const
    {
        return plan_parser->parseExpression(actions_dag, rel);
    }

    std::pair<DataTypePtr, Field> parseLiteral(const substrait::Expression_Literal & literal) const { return plan_parser->parseLiteral(literal); }

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
