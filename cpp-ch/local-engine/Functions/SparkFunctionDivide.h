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

#include <Functions/FunctionBinaryArithmetic.h>

using namespace DB;

namespace local_engine
{

class SparkFunctionDivide : public DB::FunctionBinaryArithmetic<Op, Name>
{
public:
    static constexpr auto name = "sparkDivide";
    static DB::FunctionPtr create(DB::ContextPtr) { return std::make_shared<SparkFunctionDivide>(); }
    SparkFunctionDivide() = default;
    ~SparkFunctionDivide() override = default;
    String getName() const override { return name; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override 
    {
        return nullptr;
    }
};

}
