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
#include "operators/functions/RegistrationAllFunctions.h"
#include "operators/functions/Arithmetic.h"
#include "operators/functions/RowConstructorWithNull.h"
#include "operators/functions/RowFunctionWithNull.h"

#include "velox/expression/SpecialFormRegistry.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/lib/RegistrationHelpers.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/functions/prestosql/window/WindowFunctionsRegistration.h"
#include "velox/functions/sparksql/Hash.h"
#include "velox/functions/sparksql/Register.h"
#include "velox/functions/sparksql/aggregates/Register.h"
#include "velox/functions/sparksql/window/WindowFunctionsRegistration.h"

using namespace facebook;

namespace gluten {
namespace {
void registerFunctionOverwrite() {
  velox::exec::registerStatefulVectorFunction(
      "murmur3hash",
      velox::functions::sparksql::hashWithSeedSignatures(),
      velox::functions::sparksql::makeHashWithSeed);

  velox::exec::registerStatefulVectorFunction(
      "xxhash64",
      velox::functions::sparksql::xxhash64WithSeedSignatures(),
      velox::functions::sparksql::makeXxHash64WithSeed);

  velox::functions::registerUnaryNumeric<RoundFunction>({"round"});
  velox::registerFunction<RoundFunction, int8_t, int8_t, int32_t>({"round"});
  velox::registerFunction<RoundFunction, int16_t, int16_t, int32_t>({"round"});
  velox::registerFunction<RoundFunction, int32_t, int32_t, int32_t>({"round"});
  velox::registerFunction<RoundFunction, int64_t, int64_t, int32_t>({"round"});
  velox::registerFunction<RoundFunction, double, double, int32_t>({"round"});
  velox::registerFunction<RoundFunction, float, float, int32_t>({"round"});

  velox::exec::registerVectorFunction(
      "row_constructor_with_null",
      std::vector<std::shared_ptr<velox::exec::FunctionSignature>>{},
      std::make_unique<RowFunctionWithNull>());
  velox::exec::registerFunctionCallToSpecialForm(
      RowConstructorWithNullCallToSpecialForm::kRowConstructorWithNull,
      std::make_unique<RowConstructorWithNullCallToSpecialForm>());
}
} // namespace

void registerAllFunctions() {
  // The registration order matters. Spark sql functions are registered after
  // presto sql functions to overwrite the registration for same named functions.
  velox::functions::prestosql::registerAllScalarFunctions();
  velox::functions::sparksql::registerFunctions("");
  velox::aggregate::prestosql::registerAllAggregateFunctions(
      "", true /*registerCompanionFunctions*/, false /*onlyPrestoSignatures*/, true /*overwrite*/);
  velox::functions::aggregate::sparksql::registerAggregateFunctions(
      "", true /*registerCompanionFunctions*/, true /*overwrite*/);
  velox::window::prestosql::registerAllWindowFunctions();
  velox::functions::window::sparksql::registerWindowFunctions("");
  // Using function overwrite to handle function names mismatch between Spark and Velox.
  registerFunctionOverwrite();
}

} // namespace gluten
