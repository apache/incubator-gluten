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
#include <Functions/FunctionsRound.h>
#include <Functions/FunctionFactory.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesNumber.h>
#include <bit>

namespace local_engine
{
class SparkFunctionFloor : public DB::FunctionFloor
{
public:
    static constexpr auto name = "spark_floor";
    static DB::FunctionPtr create(DB::ContextPtr) { return std::make_shared<SparkFunctionFloor>(); }
    SparkFunctionFloor() = default;
    ~SparkFunctionFloor() override = default;
    DB::String getName() const override { return name; }

    DB::DataTypePtr getReturnTypeImpl(const DB::DataTypes & arguments) const override
    {
        auto result_type = DB::FunctionFloor::getReturnTypeImpl(arguments);
        return makeNullable(result_type);
    }

    DB::ColumnPtr executeImpl(const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr & result_type, size_t input_rows) const override
    {
        DB::ColumnPtr res = DB::FunctionFloor::executeImpl(arguments, result_type, input_rows);
        if (res->isNullable())
        {
            const DB::ColumnNullable * nullable_col = assert_cast<const DB::ColumnNullable *>(res.get());
            res = nullable_col->getNestedColumnPtr();
        }
        DB::MutableColumnPtr null_map_col = DB::ColumnUInt8::create(res->size(), 0);
        DB::TypeIndex res_type_index = res->getDataType();
        switch (res_type_index)
        {
            case DB::TypeIndex::Float32: 
            {
                DB::MutableColumnPtr res_mutable = DB::IColumn::mutate(res);
                checkAndSetNullable<DB::Float32>(res_mutable, null_map_col);
                return DB::ColumnNullable::create(std::move(res_mutable), std::move(null_map_col));
            }
            case DB::TypeIndex::Float64: 
            {
                DB::MutableColumnPtr res_mutable = DB::IColumn::mutate(res);
                checkAndSetNullable<DB::Float64>(res_mutable, null_map_col);
                return DB::ColumnNullable::create(std::move(res_mutable), std::move(null_map_col));
            }
            default:
                 return DB::ColumnNullable::create(std::move(res), std::move(null_map_col));
        }
       
    }

    template<typename T>
    static void checkAndSetNullable(DB::MutableColumnPtr & data_col_ptr, DB::MutableColumnPtr & null_map_ptr)
    {
        DB::PaddedPODArray<UInt8> & null_map = assert_cast<DB::ColumnUInt8 *>(null_map_ptr.get())->getData();
        DB::PaddedPODArray<T> & data = assert_cast<DB::ColumnVector<T> *>(data_col_ptr.get())->getData();
        for (size_t i = 0; i < data.size(); ++i)
        {
            const T t = data[i];
            if (t != t) // means the element is nan
            {
                data[i] = 0;
                null_map[i] = 1;
            }
            else if constexpr (std::is_same<T, float>::value) // means the float type element is inf
            {
                if ((std::bit_cast<uint32_t>(t) & 0b01111111111111111111111111111111) == 0b01111111100000000000000000000000)
                {
                    data[i] = 0;
                    null_map[i] = 1;
                }
            }
            else if constexpr (std::is_same<T, double>::value) // means the double type element is inf
            {
                if ((std::bit_cast<uint64_t>(t) & 0b0111111111111111111111111111111111111111111111111111111111111111) 
                    == 0b0111111111110000000000000000000000000000000000000000000000000000)
                {
                    data[i] = 0;
                    null_map[i] = 1;
                }
            }
        }
    }
};

REGISTER_FUNCTION(SparkFloor)
{
    factory.registerFunction<SparkFunctionFloor>();
}

}
