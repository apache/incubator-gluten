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
#include "SparkFunctionDecimalBinaryArithmetic.h"
#include "SparkFunctionDecimalBinaryOperator.h"

namespace local_engine
{
struct NameSparkDecimalDivide
{
    static constexpr auto name = "sparkDecimalDivide";
};
struct NameSparkDecimalDivideEffect
{
    static constexpr auto name = "sparkDecimalDivideEffect";
};

using DecimalDivide = SparkFunctionDecimalBinaryArithmetic<DecimalDivideImpl, NameSparkDecimalDivide>;
using DecimalDivideEffect = SparkFunctionDecimalBinaryArithmetic<DecimalDivideImpl, NameSparkDecimalDivideEffect, OpMode::Effect>;

REGISTER_FUNCTION(SparkDecimalFunctionDivide)
{
    factory.registerFunction<DecimalDivide>();
    factory.registerFunction<DecimalDivideEffect>();
}

}
