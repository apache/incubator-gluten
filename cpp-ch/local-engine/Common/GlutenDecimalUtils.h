/*
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


namespace local_engine
{

class GlutenDecimalUtils
{
public:
    static constexpr size_t MAX_PRECISION = 38;
    static constexpr size_t MAX_SCALE = 38;
    static constexpr auto system_Default = std::tuple(MAX_PRECISION, 18);
    static constexpr auto user_Default = std::tuple(10, 0);
    static constexpr size_t MINIMUM_ADJUSTED_SCALE = 6;

    // The decimal types compatible with other numeric types
    static constexpr auto BOOLEAN_DECIMAL = std::tuple(1, 0);
    static constexpr auto BYTE_DECIMAL = std::tuple(3, 0);
    static constexpr auto SHORT_DECIMAL = std::tuple(5, 0);
    static constexpr auto INT_DECIMAL = std::tuple(10, 0);
    static constexpr auto LONG_DECIMAL = std::tuple(20, 0);
    static constexpr auto FLOAT_DECIMAL = std::tuple(14, 7);
    static constexpr auto DOUBLE_DECIMAL = std::tuple(30, 15);
    static constexpr auto BIGINT_DECIMAL = std::tuple(MAX_PRECISION, 0);

    static std::tuple<size_t, size_t> adjustPrecisionScale(size_t precision, size_t scale)
    {
        if (precision <= MAX_PRECISION)
        {
            // Adjustment only needed when we exceed max precision
            return std::tuple(precision, scale);
        }
        else if (scale < 0)
        {
            // Decimal can have negative scale (SPARK-24468). In this case, we cannot allow a precision
            // loss since we would cause a loss of digits in the integer part.
            // In this case, we are likely to meet an overflow.
            return std::tuple(GlutenDecimalUtils::MAX_PRECISION, scale);
        }
        else
        {
            // Precision/scale exceed maximum precision. Result must be adjusted to MAX_PRECISION.
            auto intDigits = precision - scale;
            // If original scale is less than MINIMUM_ADJUSTED_SCALE, use original scale value; otherwise
            // preserve at least MINIMUM_ADJUSTED_SCALE fractional digits
            auto minScaleValue = std::min(scale, GlutenDecimalUtils::MINIMUM_ADJUSTED_SCALE);
            // The resulting scale is the maximum between what is available without causing a loss of
            // digits for the integer part of the decimal and the minimum guaranteed scale, which is
            // computed above
            auto adjustedScale = std::max(GlutenDecimalUtils::MAX_PRECISION - intDigits, minScaleValue);

            return std::tuple(GlutenDecimalUtils::MAX_PRECISION, adjustedScale);
        }
    }

    static std::tuple<size_t, size_t> dividePrecisionScale(size_t p1, size_t s1, size_t p2, size_t s2, bool allowPrecisionLoss)
    {
        if (allowPrecisionLoss)
        {
            // Precision: p1 - s1 + s2 + max(6, s1 + p2 + 1)
            // Scale: max(6, s1 + p2 + 1)
            const size_t intDig = p1 - s1 + s2;
            const size_t scale = std::max(MINIMUM_ADJUSTED_SCALE, s1 + p2 + 1);
            const size_t precision = intDig + scale;
            return adjustPrecisionScale(precision, scale);
        }
        else
        {
            auto intDig = std::min(MAX_SCALE, p1 - s1 + s2);
            auto decDig = std::min(MAX_SCALE, std::max(static_cast<size_t>(6), s1 + p2 + 1));
            auto diff = (intDig + decDig) - MAX_SCALE;
            if (diff > 0)
            {
                decDig -= diff / 2 + 1;
                intDig = MAX_SCALE - decDig;
            }
            return std::tuple(intDig + decDig, decDig);
        }
    }


};

}
