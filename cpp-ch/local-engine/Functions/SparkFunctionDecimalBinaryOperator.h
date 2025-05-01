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
#include "config.h"

#include <base/arithmeticOverflow.h>

#if USE_EMBEDDED_COMPILER
#include <DataTypes/Native.h>
#include <llvm/IR/IRBuilder.h>
#endif

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wbit-int-extension"

using BitInt128 = signed _BitInt(128);
using BitUInt128 = unsigned _BitInt(128);
#if defined(__x86_64__)
using BitInt256 = signed _BitInt(256);
using BitUInt256 = unsigned _BitInt(256);
#else
// up to version 18, clang supports large _Bitint sizes on x86 and x86-64;
// but on arm and aarch64, they are currently only supported up to 128 bits.
// https://stackoverflow.com/questions/78614816/why-am-i-getting-a-256-bit-arithmetic-error-unsigined-bitint-of-bit-sizes-gre
using BitInt256 = Int256;
using BitUInt256 = UInt256;
#endif

namespace local_engine
{

static bool canCastLower(const Int256 & a, const Int256 & b)
{
    return a.items[2] == 0 && a.items[3] == 0 && b.items[2] == 0 && b.items[3] == 0;
}

static bool canCastLower(const Int128 & a, const Int128 & b)
{
    return a.items[1] == 0 && b.items[1] == 0;
}

static bool canCastLower(const UInt128 & a, const UInt128 & b)
{
    return a.items[1] == 0 && b.items[1] == 0;
}

static const Int256 & toInt256(const BitInt256 & value)
{
    return *reinterpret_cast<const Int256 *>(&value);
}

static const BitInt256 & toBitInt256(const Int256 & value)
{
    return *reinterpret_cast<const BitInt256 *>(&value);
}

/// TODO(taiyang-li): remove all overflow checking in below codes because we have already checked overflow in SparkDecimalBinaryOperation
struct DecimalPlusImpl
{
    template <typename T>
    static bool apply(T a, T b, T & r)
    {
        r = a + b;
        return true;
    }

    template <>
    static bool apply(Int128 a, Int128 b, Int128 & r)
    {
        if (canCastLower(a, b))
        {
            UInt64 low_result;
            if (!common::addOverflow(static_cast<UInt64>(a), static_cast<UInt64>(b), low_result))
            {
                r = static_cast<Int128>(low_result);
                chassert(r == a + b);
                return true;
            }
        }

        r = a + b;
        return true;
    }

    template <>
    static bool apply(Int256 a, Int256 b, Int256 & r)
    {
        if (canCastLower(a, b))
        {
            UInt128 low_result;
            if (!common::addOverflow(static_cast<UInt128>(a), static_cast<UInt128>(b), low_result))
            {
                r = static_cast<Int256>(low_result);
                chassert(r == a + b);
                return true;
            }
        }

        r = toInt256(toBitInt256(a) + toBitInt256(b));
        chassert(r == a + b);
        return true;
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool)
    {
        return left->getType()->isIntegerTy() ? b.CreateAdd(left, right) : b.CreateFAdd(left, right);
    }
#endif
};


struct DecimalMinusImpl
{
    /// Apply operation and check overflow. It's used for Deciamal operations. @returns true if overflowed, false otherwise.
    template <typename T>
    static bool apply(T a, T b, T & r)
    {
        r = a - b;
        return true;
    }

    template <>
    static bool apply(Int128 a, Int128 b, Int128 & r)
    {
        if (canCastLower(a, b))
        {
            Int64 low_result;
            if (!common::subOverflow(static_cast<Int64>(a), static_cast<Int64>(b), low_result))
            {
                r = static_cast<Int128>(low_result);
                chassert(r == a - b);
                return true;
            }
        }

        r = a - b;
        return true;
    }

    template <>
    static bool apply(Int256 a, Int256 b, Int256 & r)
    {
        if (canCastLower(a, b))
        {
            Int128 low_result;
            if (!common::subOverflow(static_cast<Int128>(a), static_cast<Int128>(b), low_result))
            {
                r = static_cast<Int256>(low_result);
                chassert(r == a - b);
                return true;
            }
        }

        r = toInt256(toBitInt256(a) - toBitInt256(b));
        chassert(r == a - b);
        return true;
    }


#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool)
    {
        return left->getType()->isIntegerTy() ? b.CreateSub(left, right) : b.CreateFSub(left, right);
    }
#endif
};


struct DecimalMultiplyImpl
{
    /// Apply operation and check overflow. It's used for Decimal operations. @returns true if overflowed, false otherwise.
    template <typename T>
    static bool apply(T a, T b, T & c)
    {
        c = a * b;
        return true;
    }

    template <>
    static bool apply(Int128 a, Int128 b, Int128 & r)
    {
        if (canCastLower(a, b))
        {
            UInt64 low_result = 0;
            if (!common::mulOverflow(static_cast<UInt64>(a), static_cast<UInt64>(b), low_result))
            {
                r = static_cast<Int128>(low_result);
                chassert(r == a * b);
                return true;
            }
        }

        r = a * b;
        return true;
    }

    template <>
    static bool apply(Int256 a, Int256 b, Int256 & r)
    {
        /// Notice that we can't use common::mulOverflow here because it doesn't support checking overflow on Int128 multiplication.
        r = toInt256(toBitInt256(a) * toBitInt256(b));
        chassert(r == a * b);
        return true;
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool)
    {
        return left->getType()->isIntegerTy() ? b.CreateMul(left, right) : b.CreateFMul(left, right);
    }
#endif
};

struct DecimalDivideImpl
{
    template <typename T>
    static bool apply(T a, T b, T & r)
    {
        if (b == 0)
            return false;

        r = a / b;
        return true;
    }

    template <>
    static bool apply(Int128 a, Int128 b, Int128 & r)
    {
        if (b == 0)
            return false;

        if (canCastLower(a, b))
        {
            /// We must cast to UInt64 to avoid overflow in the division.
            r = static_cast<Int128>(static_cast<UInt64>(a) / static_cast<UInt64>(b));
            chassert(r == a / b);
            return true;
        }

        r = a / b;
        return true;
    }

    template <>
    static bool apply(UInt128 a, UInt128 b, UInt128 & r)
    {
        if (b == 0)
            return false;

        if (canCastLower(a, b))
        {
            /// We must cast to UInt64 to avoid overflow in the division.
            r = static_cast<UInt128>(static_cast<UInt64>(a) / static_cast<UInt64>(b));
            chassert(r == a / b);
            return true;
        }

        r = a / b;
        return true;
    }


    template <>
    static bool apply(Int256 a, Int256 b, Int256 & r)
    {
        if (b == 0)
            return false;

        if (canCastLower(a, b))
        {
            /// We must cast to UInt128 to avoid overflow in the division.
            UInt128 low_result;
            apply(static_cast<UInt128>(a), static_cast<UInt128>(b), low_result);
            r = static_cast<Int256>(low_result);
            chassert(r == a / b);
            return true;
        }

        r = toInt256(toBitInt256(a) / toBitInt256(b));
        chassert(r == a / b);
        return true;
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool)
    {
        return left->getType()->isIntegerTy() ? b.CreateSub(left, right) : b.CreateFSub(left, right);
    }
#endif
};


// ModuloImpl
struct DecimalModuloImpl
{
    template <typename T>
    static bool apply(T a, T b, T & r)
    {
        if (b == 0)
            return false;

        r = a % b;
        return true;
    }

    template <>
    static bool apply(Int128 a, Int128 b, Int128 & r)
    {
        if (b == 0)
            return false;

        if (canCastLower(a, b))
        {
            /// We must cast to UInt64 to avoid overflow in the division.
            r = static_cast<Int128>(static_cast<UInt64>(a) % static_cast<UInt64>(b));
            chassert(r == a % b);
            return true;
        }

        r = a % b;
        return true;
    }


    template <>
    static bool apply(Int256 a, Int256 b, Int256 & r)
    {
        if (b == 0)
            return false;

        if (canCastLower(a, b))
        {
            /// We must cast to UInt128 to avoid overflow in the division.
            r = static_cast<Int256>(static_cast<UInt128>(a) % static_cast<UInt128>(b));
            chassert(r == a % b);
            return true;
        }

        r = toInt256(toBitInt256(a) % toBitInt256(b));
        chassert(r == a % b);
        return true;
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool)
    {
        return left->getType()->isIntegerTy() ? b.CreateSub(left, right) : b.CreateFSub(left, right);
    }
#endif
};

}
