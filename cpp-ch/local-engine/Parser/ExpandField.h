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

#include <Core/Field.h>
#include <DataTypes/IDataType.h>

namespace local_engine
{
enum ExpandFieldKind
{
    EXPAND_FIELD_KIND_SELECTION,
    EXPAND_FIELD_KIND_LITERAL,
};

class ExpandField
{
public:
    ExpandField() = default;
    ExpandField(
        const std::vector<std::string> & names_,
        const std::vector<DB::DataTypePtr> & types_,
        const std::vector<std::vector<ExpandFieldKind>> & kinds_,
        const std::vector<std::vector<DB::Field>> & fields_)
        : names(names_), types(types_), kinds(kinds_), fields(fields_)
    {
    }

    const std::vector<std::string> & getNames() const { return names; }
    const std::vector<DB::DataTypePtr> & getTypes() const { return types; }
    const std::vector<std::vector<ExpandFieldKind>> & getKinds() const { return kinds; }
    const std::vector<std::vector<DB::Field>> & getFields() const { return fields; }

    size_t getExpandRows() const { return kinds.size(); }
    size_t getExpandCols() const { return types.size(); }

private:
    std::vector<std::string> names;
    std::vector<DB::DataTypePtr> types;
    std::vector<std::vector<ExpandFieldKind>> kinds;
    std::vector<std::vector<DB::Field>> fields;
};

}
