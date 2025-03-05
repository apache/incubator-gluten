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

#include "QueryAssertions.h"

#include <Core/Block.h>
#include <Parser/SubstraitParserUtils.h>
#include <gtest/gtest.h>
#include <Common/BlockTypeUtils.h>

namespace local_engine::test
{

bool assertEqualResults(const DB::Block & actual, const DB::Block & expected, const std::string & message)
{
    if (!(expected.rows() == actual.rows() && expected.columns() == actual.columns() && sameType(expected, actual)
          && sameName(expected, actual)))
    {
        auto error = fmt::format(
            "Types of actual[{} column(s)] and expected[{} column(s)] results do not match: \n  Actual: {}\nExpected: {}",
            actual.columns(),
            expected.columns(),
            actual.dumpStructure(),
            expected.dumpStructure());
        ADD_FAILURE() << error << message;

        return false;
    }


    if (expected.rows() == 0)
        return true;


    for (size_t colIndex = 0; colIndex < expected.columns(); ++colIndex)
    {
        const auto & expectedCol = *expected.getByPosition(colIndex).column;
        const auto & actualCol = *actual.getByPosition(colIndex).column;

        for (size_t rowIndex = 0; rowIndex < expectedCol.size(); ++rowIndex)
        {
            if (expectedCol.compareAt(rowIndex, rowIndex, actualCol, 1) != 0)
            {
                auto error = fmt::format(
                    "Column \"{}\"'s value at position ({}) do not match: \n  Actual: {}\nExpected: {}",
                    expected.getByPosition(colIndex).name,
                    rowIndex,
                    toString(actualCol[rowIndex]),
                    toString(expectedCol[rowIndex]));
                ADD_FAILURE() << error << message;
                return false;
            }
        }
    }

    return true;
}

}
