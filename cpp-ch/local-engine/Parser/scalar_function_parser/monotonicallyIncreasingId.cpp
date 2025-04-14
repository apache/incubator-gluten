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
#include <DataTypes/DataTypesNumber.h>
#include <Parser/FunctionParser.h>
#include <Common/Exception.h>

namespace local_engine
{
class SparkFunctionMonotonicallyIncreasingIDParser : public FunctionParser
{
public:
    explicit SparkFunctionMonotonicallyIncreasingIDParser(ParserContextPtr parser_context_) : FunctionParser(parser_context_) { }
    ~SparkFunctionMonotonicallyIncreasingIDParser() override = default;
    static constexpr auto name = "monotonically_increasing_id";
    String getName() const { return name; }
    String getCHFunctionName(const substrait::Expression_ScalarFunction &) const override { return "sparkMonotonicallyIncreasingId"; }

    const DB::ActionsDAG::Node *
    parse(const substrait::Expression_ScalarFunction & substrait_func, DB::ActionsDAG & actions_dag) const override
    {
        const DB::DataTypePtr type = std::make_shared<DB::DataTypeInt64>();
        auto partition_index = parser_context->getPartitionIndex();

        DB::ActionsDAG::NodeRawConstPtrs args;
        args.reserve(1);
        args.emplace_back(&actions_dag.addColumn(
            DB::ColumnWithTypeAndName(type->createColumnConst(1, partition_index), type, getUniqueName("partition_index"))));

        auto ch_function_name = getCHFunctionName(substrait_func);
        const auto * func_node = toFunctionNode(actions_dag, ch_function_name, args);
        return convertNodeTypeIfNeeded(substrait_func, func_node, actions_dag);
    }
};
static FunctionParserRegister<SparkFunctionMonotonicallyIncreasingIDParser> register_monotonically_increasing_id;
}
