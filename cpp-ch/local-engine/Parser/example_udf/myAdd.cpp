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
#include <Parser/FunctionParser.h>

namespace local_engine
{

class FunctionParserMyAdd: public FunctionParser
{
public:
    explicit FunctionParserMyAdd(ParserContextPtr ctx) : FunctionParser(ctx) { }

    static constexpr auto name = "my_add";

    String getName() const override { return name; }

private:
    String getCHFunctionName(const substrait::Expression_ScalarFunction &) const override { return "plus"; }
};

static FunctionParserRegister<FunctionParserMyAdd> register_my_add;
}
