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

    virtual String getCHFunctionName(const substrait::Expression_ScalarFunction & substrait_func) const;
protected:

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
