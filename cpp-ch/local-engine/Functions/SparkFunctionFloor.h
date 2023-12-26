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

template <typename T>
requires std::is_floating_point_v<T>
static UInt8 checkAndSetNullableAutoOpt(T t)
{
    UInt8 is_nan = (t != t);
    UInt8 is_inf = 0;
    if constexpr (std::is_same_v<T, float>)
        is_inf = ((*reinterpret_cast<const uint32_t *>(&t) & 0b01111111111111111111111111111111) == 0b01111111100000000000000000000000);
    else if constexpr (std::is_same_v<T, double>)
        is_inf
            = ((*reinterpret_cast<const uint64_t *>(&t) & 0b0111111111111111111111111111111111111111111111111111111111111111)
               == 0b0111111111110000000000000000000000000000000000000000000000000000);

    return is_nan | is_inf;
}

DECLARE_AVX_SPECIFIC_CODE(

    inline void checkNullFloat32SIMD(const Float32 * data, UInt8 * null_map, size_t size) {
        __m128 inf = _mm_set1_ps(INFINITY);
        __m128 neg_inf = _mm_set1_ps(-INFINITY);

        size_t i = 0;
        for (; i + 3 < size; i += 4)
        {
            __m128 values = _mm_loadu_ps(&data[i]);

            __m128 cmp_result_inf = _mm_cmp_ps(values, inf, _CMP_EQ_OQ);
            __m128 cmp_result_neg_inf = _mm_cmp_ps(values, neg_inf, _CMP_EQ_OQ);
            __m128 cmp_result_nan = _mm_cmp_ps(values, values, _CMP_NEQ_UQ);

            __m128 cmp_result = _mm_or_ps(_mm_or_ps(cmp_result_inf, cmp_result_neg_inf), cmp_result_nan);

            int mask = _mm_movemask_ps(cmp_result);
            for (size_t j = 0; j < 4; ++j)
                null_map[i + j] = (mask & (1 << j)) != 0;
        }

        for (; i < size; ++i)
            null_map[i] = checkAndSetNullableAutoOpt(data[i]);
    }

    inline void checkNullFloat64SIMD(const Float64 * data, UInt8 * null_map, size_t size) {
        __m128d inf = _mm_set1_pd(INFINITY);
        __m128d neg_inf = _mm_set1_pd(-INFINITY);

        size_t i = 0;
        for (; i + 1 < size; i += 2)
        {
            __m128d values = _mm_loadu_pd(&data[i]);

            __m128d cmp_result_inf = _mm_cmp_pd(values, inf, _CMP_EQ_OQ);
            __m128d cmp_result_neg_inf = _mm_cmp_pd(values, neg_inf, _CMP_EQ_OQ);
            __m128d cmp_result_nan = _mm_cmp_pd(values, values, _CMP_NEQ_UQ);

            __m128d cmp_result = _mm_or_pd(_mm_or_pd(cmp_result_inf, cmp_result_neg_inf), cmp_result_nan);

            int mask = _mm_movemask_pd(cmp_result);
            for (size_t j = 0; j < 2; ++j)
                null_map[i + j] = (mask & (1 << j)) != 0;
        }

        for (; i < size; ++i)
            null_map[i] = checkAndSetNullableAutoOpt(data[i]);
    }

)

DECLARE_AVX2_SPECIFIC_CODE(

    inline void checkNullFloat32SIMD(const Float32 * data, UInt8 * null_map, size_t size) {
        __m256 inf = _mm256_set1_ps(INFINITY);
        __m256 neg_inf = _mm256_set1_ps(-INFINITY);

        size_t i = 0;
        for (; i + 7 < size; i += 8)
        {
            __m256 values = _mm256_loadu_ps(&data[i]);

            __m256 cmp_result_inf = _mm256_cmp_ps(values, inf, _CMP_EQ_OQ);
            __m256 cmp_result_neg_inf = _mm256_cmp_ps(values, neg_inf, _CMP_EQ_OQ);
            __m256 cmp_result_nan = _mm256_cmp_ps(values, values, _CMP_NEQ_UQ);

            __m256i cmp_result = _mm256_castps_si256(_mm256_or_ps(_mm256_or_ps(cmp_result_inf, cmp_result_neg_inf), cmp_result_nan));

            UInt32 mask = static_cast<UInt32>(_mm256_movemask_ps(cmp_result));
            for (size_t j = 0; j < 8; ++j)
            {
                null_map[i + j] = (mask & 1U);
                mask >>= 1;
            }


            /*
            Int32 value = 0;

#define CHECK_NULL_MAP(offset) \
            value = _mm256_extract_epi32(cmp_result, offset); \
            null_map[i + offset] = (value != 0); \


            CHECK_NULL_MAP(0);
            CHECK_NULL_MAP(1);
            CHECK_NULL_MAP(2);
            CHECK_NULL_MAP(3);
            CHECK_NULL_MAP(4);
            CHECK_NULL_MAP(5);
            CHECK_NULL_MAP(6);
            CHECK_NULL_MAP(7);
            */
        }

        for (; i < size; ++i)
            null_map[i] = checkAndSetNullableAutoOpt(data[i]);
    }

    inline void checkNullFloat64SIMD(const Float64 * data, UInt8 * null_map, size_t size) {
        __m256d inf = _mm256_set1_pd(INFINITY);
        __m256d neg_inf = _mm256_set1_pd(-INFINITY);

        size_t i = 0;
        for (; i + 3 < size; i += 4)
        {
            __m256d values = _mm256_loadu_pd(&data[i]);

            __m256d cmp_result_inf = _mm256_cmp_pd(values, inf, _CMP_EQ_OQ);
            __m256d cmp_result_neg_inf = _mm256_cmp_pd(values, neg_inf, _CMP_EQ_OQ);
            __m256d cmp_result_nan = _mm256_cmp_pd(values, values, _CMP_NEQ_UQ);

            __m256 cmp_result = _mm256_or_pd(_mm256_or_pd(cmp_result_inf, cmp_result_neg_inf), cmp_result_nan);

            UInt32 mask = static_cast<UInt32>(_mm256_movemask_pd(cmp_result));
            for (size_t j = 0; j < 4; ++j)
            {
                null_map[i + j] = (mask & 1U);
                mask >>= 1;
            }

            /*
            Int64 value = 0;

#define CHECK_NULL_MAP(offset) \
            value = _mm256_extract_epi64 (cmp_result, offset); \
            null_map[i + offset] = (value != 0); \

            CHECK_NULL_MAP(0);
            CHECK_NULL_MAP(1);
            CHECK_NULL_MAP(2);
            CHECK_NULL_MAP(3);
            */
        }

        for (; i < size; ++i)
            null_map[i] = checkAndSetNullableAutoOpt(data[i]);
    }

)

DECLARE_AVX512F_SPECIFIC_CODE(

    inline void checkNullFloat32SIMD(const Float32 * data, UInt8 * null_map, size_t size) {
        __m512 inf = _mm512_set1_ps(INFINITY);
        __m512 neg_inf = _mm512_set1_ps(-INFINITY);

        size_t i = 0;
        for (; i + 15 < size; i += 16)
        {
            __m512 values = _mm512_loadu_ps(&data[i]);

            __mmask16 cmp_result_inf = _mm512_cmp_ps_mask(values, inf, _CMP_EQ_OQ);
            __mmask16 cmp_result_neg_inf = _mm512_cmp_ps_mask(values, neg_inf, _CMP_EQ_OQ);
            __mmask16 cmp_result_nan = _mm512_cmp_ps_mask(values, values, _CMP_NEQ_UQ);

            __mmask16 cmp_result = cmp_result_inf | cmp_result_neg_inf | cmp_result_nan;

            for (size_t j = 0; j < 16; ++j)
                null_map[i + j] = (cmp_result & (1 << j)) != 0;
        }

        for (; i < size; ++i)
            null_map[i] = checkAndSetNullableAutoOpt(data[i]);
    }


    inline void checkNullFloat64SIMD(const Float64 * data, UInt8 * null_map, size_t size) {
        __m512d inf = _mm512_set1_pd(INFINITY);
        __m512d neg_inf = _mm512_set1_pd(-INFINITY);

        size_t i = 0;
        for (; i + 7 < size; i += 8)
        {
            __m512d values = _mm512_loadu_pd(&data[i]);

            __mmask8 cmp_result_inf = _mm512_cmp_pd_mask(values, inf, _CMP_EQ_OQ);
            __mmask8 cmp_result_neg_inf = _mm512_cmp_pd_mask(values, neg_inf, _CMP_EQ_OQ);
            __mmask8 cmp_result_nan = _mm512_cmp_pd_mask(values, values, _CMP_NEQ_UQ);

            __mmask8 cmp_result = cmp_result_inf | cmp_result_neg_inf | cmp_result_nan;

            for (size_t j = 0; j < 8; ++j)
                null_map[i + j] = (cmp_result & (1 << j)) != 0;
        }

        for (; i < size; ++i)
            null_map[i] = checkAndSetNullableAutoOpt(data[i]);
    }
)


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

        if constexpr (std::is_same_v<T, Float32>)
        {
            TargetSpecific::AVX2::checkNullFloat32SIMD(out.data(), null_map.data(), out.size());
        }
        else if constexpr (std::is_same_v<T, Float64>)
        {
            TargetSpecific::AVX2::checkNullFloat64SIMD(out.data(), null_map.data(), out.size());
        }

        /*
        for (size_t i = 0; i < out.size(); ++i)
            null_map[i] = checkAndSetNullableAutoOpt(out[i]);
        */
    }


    static void checkAndSetNullable(T & t, UInt8 & null_flag)
    {
        UInt8 is_nan = (t != t);
        UInt8 is_inf = 0;
        if constexpr (std::is_same_v<T, float>)
            is_inf = ((*reinterpret_cast<const uint32_t *>(&t) & 0b01111111111111111111111111111111) == 0b01111111100000000000000000000000);
        else if constexpr (std::is_same_v<T, double>)
            is_inf
                = ((*reinterpret_cast<const uint64_t *>(&t) & 0b0111111111111111111111111111111111111111111111111111111111111111)
                   == 0b0111111111110000000000000000000000000000000000000000000000000000);

        null_flag = is_nan | is_inf;
        if (null_flag) t = 0;
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
