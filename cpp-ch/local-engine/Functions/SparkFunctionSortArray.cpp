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
#include <Functions/SparkFunctionSortArray.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

template <bool positive>
struct Less
{
    const IColumn & column;

    explicit Less(const IColumn & column_) : column(column_) { }

    bool operator()(size_t lhs, size_t rhs) const
    {
        if constexpr (positive)
            /*
                Note: We use nan_direction_hint=-1 for ascending sort to make NULL the least value.
                However, NaN is also considered the least value, 
                which results in different sorting results compared to Spark since Spark treats NaN as the greatest value.
                For now, we are temporarily ignoring this issue because cases with NaN are rare,
                and aligning with Spark would require tricky modifications to the CH underlying code.
            */
            return column.compareAt(lhs, rhs, column, -1) < 0;
        else
            return column.compareAt(lhs, rhs, column, -1) > 0;
    }
};

}

template <bool positive>
ColumnPtr SparkSortArrayImpl<positive>::execute(
    const ColumnArray & array,
    ColumnPtr mapped,
    const ColumnWithTypeAndName * fixed_arguments [[maybe_unused]])
{
    const ColumnArray::Offsets & offsets = array.getOffsets();

    size_t size = offsets.size();
    size_t nested_size = array.getData().size();
    IColumn::Permutation permutation(nested_size);

    for (size_t i = 0; i < nested_size; ++i)
        permutation[i] = i;

    ColumnArray::Offset current_offset = 0;
    for (size_t i = 0; i < size; ++i)
    {
        auto next_offset = offsets[i];
        ::sort(&permutation[current_offset], &permutation[next_offset], Less<positive>(*mapped));
        current_offset = next_offset;
    }

    return ColumnArray::create(array.getData().permute(permutation, 0), array.getOffsetsPtr());
}

REGISTER_FUNCTION(SortArraySpark)
{
    factory.registerFunction<SparkFunctionSortArray>();
    factory.registerFunction<SparkFunctionReverseSortArray>();
}

}
