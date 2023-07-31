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
#include <Columns/ColumnSet.h>
#include <DataTypes/DataTypeSet.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Set.h>
#include <Parser/SerializedPlanParser.h>
#include <gtest/gtest.h>
#include <Common/DebugUtils.h>

TEST(TestFuntion, Hash)
{
    using namespace DB;
    auto & factory = FunctionFactory::instance();
    auto function = factory.get("murmurHash2_64", local_engine::SerializedPlanParser::global_context);
    auto type0 = DataTypeFactory::instance().get("String");
    auto column0 = type0->createColumn();
    column0->insert("A");
    column0->insert("A");
    column0->insert("B");
    column0->insert("c");

    auto column1 = type0->createColumn();
    column1->insert("X");
    column1->insert("X");
    column1->insert("Y");
    column1->insert("Z");

    ColumnsWithTypeAndName columns
        = {ColumnWithTypeAndName(std::move(column0), type0, "string0"), ColumnWithTypeAndName(std::move(column1), type0, "string0")};
    Block block(columns);
    std::cerr << "input:\n";
    debug::headBlock(block);
    auto executable = function->build(block.getColumnsWithTypeAndName());
    auto result = executable->execute(block.getColumnsWithTypeAndName(), executable->getResultType(), block.rows());
    std::cerr << "output:\n";
    debug::headColumn(result);
    ASSERT_EQ(result->getUInt(0), result->getUInt(1));
}

TEST(TestFunction, In)
{
    using namespace DB;
    auto & factory = FunctionFactory::instance();
    auto function = factory.get("in", local_engine::SerializedPlanParser::global_context);
    auto type0 = DataTypeFactory::instance().get("String");
    auto type_set = std::make_shared<DataTypeSet>();


    auto column1 = type0->createColumn();
    column1->insert("X");
    column1->insert("X");
    column1->insert("Y");
    column1->insert("Z");

    SizeLimits limit;
    auto set = std::make_shared<Set>(limit, true, false);
    Block col1_set_block;
    auto col1_set = type0->createColumn();
    col1_set->insert("X");
    col1_set->insert("Y");

    col1_set_block.insert(ColumnWithTypeAndName(std::move(col1_set), type0, "string0"));
    set->setHeader(col1_set_block.getColumnsWithTypeAndName());
    set->insertFromBlock(col1_set_block.getColumnsWithTypeAndName());
    set->finishInsert();
    auto future_set = std::make_shared<FutureSetFromStorage>(std::move(set));
    auto arg = ColumnSet::create(1, future_set);

    ColumnsWithTypeAndName columns
        = {ColumnWithTypeAndName(std::move(column1), type0, "string0"), ColumnWithTypeAndName(std::move(arg), type_set, "__set")};
    Block block(columns);
    std::cerr << "input:\n";
    debug::headBlock(block);
    auto executable = function->build(block.getColumnsWithTypeAndName());
    auto result = executable->execute(block.getColumnsWithTypeAndName(), executable->getResultType(), block.rows());
    std::cerr << "output:\n";
    debug::headColumn(result);
    ASSERT_EQ(result->getUInt(3), 0);
}


TEST(TestFunction, NotIn1)
{
    using namespace DB;
    auto & factory = FunctionFactory::instance();
    auto function = factory.get("notIn", local_engine::SerializedPlanParser::global_context);
    auto type0 = DataTypeFactory::instance().get("String");
    auto type_set = std::make_shared<DataTypeSet>();


    auto column1 = type0->createColumn();
    column1->insert("X");
    column1->insert("X");
    column1->insert("Y");
    column1->insert("Z");

    SizeLimits limit;
    auto set = std::make_shared<Set>(limit, true, false);
    Block col1_set_block;
    auto col1_set = type0->createColumn();
    col1_set->insert("X");
    col1_set->insert("Y");

    col1_set_block.insert(ColumnWithTypeAndName(std::move(col1_set), type0, "string0"));
    set->setHeader(col1_set_block.getColumnsWithTypeAndName());
    set->insertFromBlock(col1_set_block.getColumnsWithTypeAndName());
    set->finishInsert();
    auto future_set = std::make_shared<FutureSetFromStorage>(std::move(set));
    auto arg = ColumnSet::create(1,future_set);

    ColumnsWithTypeAndName columns
        = {ColumnWithTypeAndName(std::move(column1), type0, "string0"), ColumnWithTypeAndName(std::move(arg), type_set, "__set")};
    Block block(columns);
    std::cerr << "input:\n";
    debug::headBlock(block);
    auto executable = function->build(block.getColumnsWithTypeAndName());
    auto result = executable->execute(block.getColumnsWithTypeAndName(), executable->getResultType(), block.rows());
    std::cerr << "output:\n";
    debug::headColumn(result);
    ASSERT_EQ(result->getUInt(3), 1);
}

TEST(TestFunction, NotIn2)
{
    using namespace DB;
    auto & factory = FunctionFactory::instance();
    auto function = factory.get("in", local_engine::SerializedPlanParser::global_context);
    auto type0 = DataTypeFactory::instance().get("String");
    auto type_set = std::make_shared<DataTypeSet>();


    auto column1 = type0->createColumn();
    column1->insert("X");
    column1->insert("X");
    column1->insert("Y");
    column1->insert("Z");

    SizeLimits limit;
    auto set = std::make_shared<Set>(limit, true, false);
    Block col1_set_block;
    auto col1_set = type0->createColumn();
    col1_set->insert("X");
    col1_set->insert("Y");

    col1_set_block.insert(ColumnWithTypeAndName(std::move(col1_set), type0, "string0"));
    set->setHeader(col1_set_block.getColumnsWithTypeAndName());
    set->insertFromBlock(col1_set_block.getColumnsWithTypeAndName());
    set->finishInsert();
    auto future_set = std::make_shared<FutureSetFromStorage>(std::move(set));
    auto arg = ColumnSet::create(1,future_set);

    ColumnsWithTypeAndName columns
        = {ColumnWithTypeAndName(std::move(column1), type0, "string0"), ColumnWithTypeAndName(std::move(arg), type_set, "__set")};
    Block block(columns);
    std::cerr << "input:\n";
    debug::headBlock(block);
    auto executable = function->build(block.getColumnsWithTypeAndName());
    auto result = executable->execute(block.getColumnsWithTypeAndName(), executable->getResultType(), block.rows());

    auto function_not = factory.get("not", local_engine::SerializedPlanParser::global_context);
    auto type_bool = DataTypeFactory::instance().get("UInt8");
    ColumnsWithTypeAndName columns2 = {ColumnWithTypeAndName(result, type_bool, "string0")};
    Block block2(columns2);
    auto executable2 = function_not->build(block2.getColumnsWithTypeAndName());
    auto result2 = executable2->execute(block2.getColumnsWithTypeAndName(), executable2->getResultType(), block2.rows());
    std::cerr << "output:\n";
    debug::headColumn(result2);
    ASSERT_EQ(result2->getUInt(3), 1);
}
