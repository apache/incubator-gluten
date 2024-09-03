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
#include <AggregateFunctions/WindowFunction.h>

namespace local_engine
{
class WindowFunctionTopRowNumber : public DB::WindowFunction
{
public:
    explicit WindowFunctionTopRowNumber(const String name, const DB::DataTypes & arg_types_, const DB::Array & parameters_);
    ~WindowFunctionTopRowNumber() override = default;

    void windowInsertResultInto(const DB::WindowTransform * transform, size_t function_index) const override;
    bool allocatesMemoryInArena() const override { return false; }

private:
    size_t limit = 0;
};
}
