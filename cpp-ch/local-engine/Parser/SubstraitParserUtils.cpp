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
#include <Parser/SubstraitParserUtils.h>
#include "substrait/algebra.pb.h"
namespace local_engine::SubstraitParserUtils
{
std::optional<size_t> getStructFieldIndex(const substrait::Expression & e)
{
    if (!e.has_selection())
        return {};
    const auto & select = e.selection();
    if (!select.has_direct_reference())
        return {};
    const auto & ref = select.direct_reference();
    if (!ref.has_struct_field())
        return {};
    return ref.struct_field().field();
}

substrait::Expression buildStructFieldExpression(size_t index)
{
    substrait::Expression e;
    e.mutable_selection()->mutable_direct_reference()->mutable_struct_field()->set_field(index);
    return e;
}

}
