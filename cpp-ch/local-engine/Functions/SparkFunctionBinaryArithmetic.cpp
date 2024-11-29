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
#include <Functions/FunctionBinaryArithmetic.h>
#include <Functions/FunctionFactory.h>

#include <libdivide.h>
#include <libdivide-config.h>

#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_DIVISION;
}
}
namespace local_engine
{
/// Optimizations for integer modulo by a constant.

template <typename A, typename B>
struct ModuloByConstantImpl : DB::BinaryOperation<A, B, DB::ModuloImpl<A, B>>
{
    using Op = DB::ModuloImpl<A, B>;
    using ResultType = typename Op::ResultType;
    static const constexpr bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = false;

    template <DB::OpCase op_case>
    static void NO_INLINE
    process(const A * __restrict a, const B * __restrict b, ResultType * __restrict c, size_t size, const DB::NullMap * right_nullmap)
    {
        if constexpr (op_case == DB::OpCase::RightConstant)
        {
            if (right_nullmap && (*right_nullmap)[0])
                return;
            vectorConstant(a, *b, c, size);
        }
        else
        {
            if (right_nullmap)
            {
                for (size_t i = 0; i < size; ++i)
                    if ((*right_nullmap)[i])
                        c[i] = ResultType();
                    else
                        apply<op_case>(a, b, c, i);
            }
            else
                for (size_t i = 0; i < size; ++i)
                    apply<op_case>(a, b, c, i);
        }
    }

    static ResultType process(A a, B b) { return Op::template apply<ResultType>(a, b); }

    static void NO_INLINE NO_SANITIZE_UNDEFINED vectorConstant(const A * __restrict src, B b, ResultType * __restrict dst, size_t size)
    {
        /// Modulo with too small divisor.
        if (unlikely((std::is_signed_v<B> && b == -1) || b == 1))
        {
            for (size_t i = 0; i < size; ++i)
                dst[i] = 0;
            return;
        }

        /// Modulo with too large divisor.
        if (unlikely(
                b > std::numeric_limits<A>::max() || (std::is_signed_v<A> && std::is_signed_v<B> && b < std::numeric_limits<A>::lowest())))
        {
            for (size_t i = 0; i < size; ++i)
                dst[i] = static_cast<ResultType>(src[i]);
            return;
        }

        if (unlikely(static_cast<A>(b) == 0))
            throw DB::Exception(DB::ErrorCodes::ILLEGAL_DIVISION, "Division by zero");

        /// Division by min negative value.
        if (std::is_signed_v<B> && b == std::numeric_limits<B>::lowest())
            throw DB::Exception(DB::ErrorCodes::ILLEGAL_DIVISION, "Division by the most negative number");

        /// Modulo of division by negative number is the same as the positive number.
        if (b < 0)
            b = -b;

        /// Here we failed to make the SSE variant from libdivide give an advantage.
        libdivide::divider<A> divider(static_cast<A>(b));
        for (size_t i = 0; i < size; ++i)
        {
            /// NOTE: perhaps, the division semantics with the remainder of negative numbers is not preserved.
            dst[i] = static_cast<ResultType>(src[i] - (src[i] / divider) * b);
        }
    }

private:
    template <DB::OpCase op_case>
    static void apply(const A * __restrict a, const B * __restrict b, ResultType * __restrict c, size_t i)
    {
        if constexpr (op_case == DB::OpCase::Vector)
            c[i] = Op::template apply<ResultType>(a[i], b[i]);
        else
            c[i] = Op::template apply<ResultType>(*a, b[i]);
    }
};
}

namespace DB
{
namespace impl_
{
template <>
struct BinaryOperationImpl<UInt64, UInt8, ModuloImpl<UInt64, UInt8>> : local_engine::ModuloByConstantImpl<UInt64, UInt8>
{
};
template <>
struct BinaryOperationImpl<UInt64, UInt16, ModuloImpl<UInt64, UInt16>> : local_engine::ModuloByConstantImpl<UInt64, UInt16>
{
};
template <>
struct BinaryOperationImpl<UInt64, UInt32, ModuloImpl<UInt64, UInt32>> : local_engine::ModuloByConstantImpl<UInt64, UInt32>
{
};
template <>
struct BinaryOperationImpl<UInt64, UInt64, ModuloImpl<UInt64, UInt64>> : local_engine::ModuloByConstantImpl<UInt64, UInt64>
{
};

template <>
struct BinaryOperationImpl<UInt32, UInt8, ModuloImpl<UInt32, UInt8>> : local_engine::ModuloByConstantImpl<UInt32, UInt8>
{
};
template <>
struct BinaryOperationImpl<UInt32, UInt16, ModuloImpl<UInt32, UInt16>> : local_engine::ModuloByConstantImpl<UInt32, UInt16>
{
};
template <>
struct BinaryOperationImpl<UInt32, UInt32, ModuloImpl<UInt32, UInt32>> : local_engine::ModuloByConstantImpl<UInt32, UInt32>
{
};
template <>
struct BinaryOperationImpl<UInt32, UInt64, ModuloImpl<UInt32, UInt64>> : local_engine::ModuloByConstantImpl<UInt32, UInt64>
{
};

template <>
struct BinaryOperationImpl<Int64, Int8, ModuloImpl<Int64, Int8>> : local_engine::ModuloByConstantImpl<Int64, Int8>
{
};
template <>
struct BinaryOperationImpl<Int64, Int16, ModuloImpl<Int64, Int16>> : local_engine::ModuloByConstantImpl<Int64, Int16>
{
};
template <>
struct BinaryOperationImpl<Int64, Int32, ModuloImpl<Int64, Int32>> : local_engine::ModuloByConstantImpl<Int64, Int32>
{
};
template <>
struct BinaryOperationImpl<Int64, Int64, ModuloImpl<Int64, Int64>> : local_engine::ModuloByConstantImpl<Int64, Int64>
{
};

template <>
struct BinaryOperationImpl<Int32, Int8, ModuloImpl<Int32, Int8>> : local_engine::ModuloByConstantImpl<Int32, Int8>
{
};
template <>
struct BinaryOperationImpl<Int32, Int16, ModuloImpl<Int32, Int16>> : local_engine::ModuloByConstantImpl<Int32, Int16>
{
};
template <>
struct BinaryOperationImpl<Int32, Int32, ModuloImpl<Int32, Int32>> : local_engine::ModuloByConstantImpl<Int32, Int32>
{
};
template <>
struct BinaryOperationImpl<Int32, Int64, ModuloImpl<Int32, Int64>> : local_engine::ModuloByConstantImpl<Int32, Int64>
{
};
}

struct SparkNameModulo
{
    static constexpr auto name = "spark_modulo";
};

/// Its JIT is implemented in ModuloImpl.
using SparkFunctionModulo = DB::BinaryArithmeticOverloadResolver<ModuloImpl, SparkNameModulo, false>;

REGISTER_FUNCTION(SparkModulo)
{
    factory.registerFunction<SparkFunctionModulo>();
}
}
