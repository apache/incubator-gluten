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

#include <Parser/scalar_function_parser/arrayElement.h>

namespace local_engine
{
    class FunctionParserGetArrayItem : public FunctionParserArrayElement
    {
    public:
        explicit FunctionParserGetArrayItem(SerializedPlanParser * plan_parser_) : FunctionParserArrayElement(plan_parser_) { }
        ~FunctionParserGetArrayItem() override = default;
        static constexpr auto name = "get_array_item";
        String getName() const override { return name; }
    };

    static FunctionParserRegister<FunctionParserGetArrayItem> register_get_array_item;
}
