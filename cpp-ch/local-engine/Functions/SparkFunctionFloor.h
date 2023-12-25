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
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypesNumber.h>
#include <bit>

using namespace DB;

namespace local_engine
{

template <typename T, ScaleMode scale_mode>
struct SparkFloatFloorImpl
{
private:
    static_assert(!is_decimal<T>);
    using Op = FloatRoundingComputation<T, RoundingMode::Floor, scale_mode>;
    using Data = std::array<T, Op::data_count>;
public:
    static NO_INLINE void apply(const PaddedPODArray<T> & in, size_t scale, PaddedPODArray<T> & out, PaddedPODArray<UInt8> & null_map)
    {
        auto mm_scale = Op::prepare(scale);
        const size_t data_count = std::tuple_size<Data>();
        const T* end_in = in.data() + in.size();
        const T* limit = in.data() + in.size() / data_count * data_count;
        const T* __restrict p_in = in.data();
        T* __restrict p_out = out.data();
        while (p_in < limit)
        {
            Op::compute(p_in, mm_scale, p_out);
            p_in += data_count;
            p_out += data_count;
        }

        if (p_in < end_in)
        {
            Data tmp_src{{}};
            Data tmp_dst;
            size_t tail_size_bytes = (end_in - p_in) * sizeof(*p_in);
            memcpy(&tmp_src, p_in, tail_size_bytes);
            Op::compute(reinterpret_cast<T *>(&tmp_src), mm_scale, reinterpret_cast<T *>(&tmp_dst));
            memcpy(p_out, &tmp_dst, tail_size_bytes);
        }
        for (size_t i = 0; i < out.size(); ++i)
            checkAndSetNullable(out[i], null_map[i]);
    }

    static void checkAndSetNullable(T& t, UInt8& null_flag)
    {
        if (t != t) // means the element is nan
        {
            t = 0;
            null_flag = 1;
        }
        else if constexpr (std::is_same<T, float>::value) // means the float type element is inf
        {
            if ((std::bit_cast<uint32_t>(t) & 0b01111111111111111111111111111111) == 0b01111111100000000000000000000000)
            {
                t = 0;
                null_flag = 1;
            }
        }
        else if constexpr (std::is_same<T, double>::value) // means the double type element is inf
        {
            if ((std::bit_cast<uint64_t>(t) & 0b0111111111111111111111111111111111111111111111111111111111111111) 
                == 0b0111111111110000000000000000000000000000000000000000000000000000)
            {
                t = 0;
                null_flag = 1;
            }
        }
    }
};

class SparkFunctionFloor : public DB::FunctionFloor
{
public:
    static constexpr auto name = "sparkFloor";
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
        const ColumnWithTypeAndName & first_arg = arguments[0];
        Scale scale_arg = getScaleArg(arguments);
        switch(first_arg.type->getTypeId())
        {
            case TypeIndex::Float32:
                return executeInternal<Float32>(first_arg.column, scale_arg);
            case TypeIndex::Float64:
                return executeInternal<Float64>(first_arg.column, scale_arg);
            default:
                DB::ColumnPtr res = DB::FunctionFloor::executeImpl(arguments, result_type, input_rows);
                DB::MutableColumnPtr null_map_col = DB::ColumnUInt8::create(first_arg.column->size(), 0);
                return DB::ColumnNullable::create(std::move(res), std::move(null_map_col));
        }
    }

    template<typename T>
    static ColumnPtr executeInternal(const ColumnPtr & col_arg, const Scale & scale_arg)
    {
        const auto * col = checkAndGetColumn<ColumnVector<T>>(col_arg.get());
        auto col_res = ColumnVector<T>::create(col->size());
        MutableColumnPtr null_map_col = DB::ColumnUInt8::create(col->size(), 0);
        PaddedPODArray<T> & vec_res = col_res->getData();
        PaddedPODArray<UInt8> & null_map_data = assert_cast<ColumnVector<UInt8> *>(null_map_col.get())->getData();
        if (!vec_res.empty())
        {
            if (scale_arg == 0)
            {
                size_t scale = 1;
                SparkFloatFloorImpl<T, ScaleMode::Zero>::apply(col->getData(), scale, vec_res, null_map_data);
            }
            else if (scale_arg > 0)
            {
                size_t scale = intExp10(scale_arg);
                SparkFloatFloorImpl<T, ScaleMode::Positive>::apply(col->getData(), scale, vec_res, null_map_data);
            }
            else
            {
                size_t scale = intExp10(-scale_arg);
                SparkFloatFloorImpl<T, ScaleMode::Negative>::apply(col->getData(), scale, vec_res, null_map_data);
            }
        }
        return DB::ColumnNullable::create(std::move(col_res), std::move(null_map_col));
    }
};
}
