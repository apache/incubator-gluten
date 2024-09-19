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

#include <DataTypes/IDataType.h>
#include <substrait/plan.pb.h>

namespace dbms
{
enum Function
{
    IS_NOT_NULL = 0,
    GREATER_THAN_OR_EQUAL,
    AND,
    LESS_THAN_OR_EQUAL,
    LESS_THAN,
    MULTIPLY,
    SUM,
    TO_DATE,
    EQUAL_TO
};

using SchemaPtr = substrait::NamedStruct *;


class SerializedPlanBuilder
{
private:
    friend class FunctionExecutor;

public:
    SerializedPlanBuilder();

    SerializedPlanBuilder & registerSupportedFunctions()
    {
        this->registerFunction(IS_NOT_NULL, "is_not_null")
            .registerFunction(GREATER_THAN_OR_EQUAL, "gte")
            .registerFunction(AND, "and")
            .registerFunction(LESS_THAN_OR_EQUAL, "lte")
            .registerFunction(LESS_THAN, "lt")
            .registerFunction(MULTIPLY, "multiply")
            .registerFunction(SUM, "sum")
            .registerFunction(TO_DATE, "to_date")
            .registerFunction(EQUAL_TO, "equal");
        return *this;
    }
    SerializedPlanBuilder & registerFunction(int id, const std::string & name);
    SerializedPlanBuilder & filter(substrait::Expression * condition);
    SerializedPlanBuilder & project(const std::vector<substrait::Expression *> & projections);
    SerializedPlanBuilder & aggregate(const std::vector<int32_t> & keys, const std::vector<substrait::AggregateRel_Measure *> & aggregates);
    SerializedPlanBuilder & read(const std::string & path, SchemaPtr schema);
    std::unique_ptr<substrait::Plan> build();

    static std::shared_ptr<substrait::Type> buildType(const DB::DataTypePtr & ch_type);
    static void buildType(const DB::DataTypePtr & ch_type, String & substrait_type);

private:
    void setInputToPrev(substrait::Rel * input);
    substrait::Rel * prev_rel = nullptr;
    std::unique_ptr<substrait::Plan> plan;
};


using Type = substrait::Type;
/**
 * build a schema, need define column name and column.
 * 1. column name
 * 2. column type
 * 3. nullability
 */
class SerializedSchemaBuilder
{
public:
    SerializedSchemaBuilder();
    SchemaPtr build();
    SerializedSchemaBuilder & column(const std::string & name, const std::string & type, bool nullable = false);

private:
    std::map<std::string, std::string> type_map;
    std::map<std::string, bool> nullability_map;
    SchemaPtr schema;
};

using ExpressionList = std::vector<substrait::Expression *>;
using MeasureList = std::vector<substrait::AggregateRel_Measure *>;


substrait::Expression * scalarFunction(int32_t id, ExpressionList args);
substrait::AggregateRel_Measure * measureFunction(int32_t id, ExpressionList args);

substrait::Expression * literal(double_t value);
substrait::Expression * literal(int32_t value);
substrait::Expression * literal(const std::string & value);
substrait::Expression * literalDate(int32_t value);

substrait::Expression * selection(int32_t field_id);

}
