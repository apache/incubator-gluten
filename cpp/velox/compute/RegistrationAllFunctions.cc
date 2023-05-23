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
#include "RegistrationAllFunctions.h"
#include "RowConstructor.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/functions/prestosql/window/WindowFunctionsRegistration.h"
#include "velox/functions/sparksql/Register.h"
#include "velox/functions/sparksql/aggregates/Register.h"
#include "velox/functions/sparksql/windows/Register.h"

using namespace facebook;

namespace gluten {

namespace {
void registerCustomFunctions() {
  velox::exec::registerVectorFunction(
      "row_constructor",
      std::vector<std::shared_ptr<velox::exec::FunctionSignature>>{},
      std::make_unique<RowConstructor>());
}
} // anonymous namespace

void registerAllFunctions() {
  // The registration order matters. Spark sql functions are registered after
  // presto sql functions to overwrite the registration for same named functions.
  velox::functions::prestosql::registerAllScalarFunctions();
  velox::functions::sparksql::registerFunctions("");
  // registerCustomFunctions();
  velox::aggregate::prestosql::registerAllAggregateFunctions();
  velox::functions::aggregate::sparksql::registerAggregateFunctions("");
  velox::window::prestosql::registerAllWindowFunctions();
  velox::functions::sparksql::windows::registerWindowFunctions("");
}

} // namespace gluten
