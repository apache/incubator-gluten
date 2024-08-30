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

#include <vector>
#include "udf/UdfLoader.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/parse/TypeResolver.h"

using namespace facebook::velox::functions::test;
using namespace facebook::velox;
class MyUdfTest : public FunctionBaseTest {
 protected:
  static void SetUpTestCase() {
    parse::registerTypeResolver();
    auto udfLoader = gluten::UdfLoader::getInstance();
    udfLoader->loadUdfLibraries("../udf/examples/libmyudf.so");
    udfLoader->registerUdf();
    memory::MemoryManager::testingSetInstance({});
  }
};

TEST_F(MyUdfTest, hivestringstring) {
  auto map = facebook::velox::exec::vectorFunctionFactories();
  const std::string candidate = {"org.apache.spark.sql.hive.execution.UDFStringString"};
  ASSERT(map.withRLock([&candidate](auto& self) -> bool {
    auto iter = self.find(candidate);
    std::unordered_map<std::string, std::string> values;
    const facebook::velox::core::QueryConfig config(std::move(values));
    iter->second.factory(
        candidate,
        {facebook::velox::exec::VectorFunctionArg{facebook::velox::VARCHAR()},
         facebook::velox::exec::VectorFunctionArg{facebook::velox::VARCHAR()}},
        config) != nullptr;
  });)
}