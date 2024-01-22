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
#include <Functions/castTypeToEither.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeNumberBase.h>
#include <DataTypes/DataTypeNullable.h>

using namespace DB;

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}
}

namespace local_engine
{

template <template <typename, typename> class Op, typename Name>
class SparkFunctionDivide : public DB::FunctionBinaryArithmetic<Op, Name>
{
public:
    SparkFunctionDivide(ContextPtr context) : FunctionBinaryArithmetic<Op, Name>(context) {}
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return makeNullable(FunctionBinaryArithmetic<Op, Name>::getReturnTypeImpl(arguments));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (arguments.size() != 2)
            throw Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {}'s arguments number must be 2", Name::name);
        if (!isNativeNumber(arguments[0].type) || !isNativeNumber(arguments[1].type))
        {
            return FunctionBinaryArithmetic<Op, Name>::executeImpl(arguments, result_type, input_rows_count);
        }
        
        ColumnPtr result = nullptr;

        using Types = TypeList<DataTypeFloat32, DataTypeFloat64,DataTypeUInt8, DataTypeUInt16, DataTypeUInt32,
            DataTypeUInt64, DataTypeInt8, DataTypeInt16, DataTypeInt32, DataTypeInt64>;
        bool valid = castTypeToEither(Types{}, arguments[0].type.get(), [&](const auto & left_)
        {
            return castTypeToEither(Types{}, arguments[1].type.get(), [&](const auto & right_)
            {
                using L = typename std::decay_t<decltype(left_)>::FieldType;
                using R = typename std::decay_t<decltype(right_)>::FieldType;
                using T = typename NumberTraits::ResultOfFloatingPointDivision<L, R>::Type;
                const ColumnVector<L> * vec1 = assert_cast<const ColumnVector<L> *>(arguments[0].column.get());
                const ColumnVector<R> * vec2 = assert_cast<const ColumnVector<R> *>(arguments[1].column.get());
                auto vec3 = ColumnVector<T>::create(vec1->size());
                auto null_map_col = ColumnVector<UInt8>::create(vec1->size(), 0);
                PaddedPODArray<T> & data = vec3->getData();
                PaddedPODArray<UInt8> & null_map = null_map_col->getData();
                for (size_t i = 0; i < vec1->size(); ++i)
                {
                    L l = vec1->getElement(i);
                    R r = vec2->getElement(i);
                    if (r == 0)
                    {
                        data[i] = 0;
                        null_map[i] = 1;
                    }
                    else
                        data[i] = Op<L,R>::apply(l, r);
                }
                result = ColumnNullable::create(std::move(vec3), std::move(null_map_col));
                return true;
            });
        });
        if (!valid)
            throw Exception(DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {}'s arguments type is not valid", Name::name);
        
        return result;
    }
};

template <template <typename, typename> class Op, typename Name>
class SparkFunctionDivideWithConstants : public DB::FunctionBinaryArithmeticWithConstants<Op, Name>
{
public:
    SparkFunctionDivideWithConstants(
        const ColumnWithTypeAndName & left_,
        const ColumnWithTypeAndName & right_,
        const DataTypePtr & return_type_,
        ContextPtr context_) : FunctionBinaryArithmeticWithConstants<Op,Name>(left_,right_,return_type_,context_){}
    
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return makeNullable(FunctionBinaryArithmeticWithConstants<Op, Name>::getReturnTypeImpl(arguments));
    }
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (arguments.size() != 2)
            throw Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {}'s arguments number must be 2", Name::name);
        if (!isNativeNumber(arguments[0].type) || !isNativeNumber(arguments[1].type))
        {
            return FunctionBinaryArithmetic<Op, Name>::executeImpl(arguments, result_type, input_rows_count);
        }
        using Types = TypeList<DataTypeFloat32, DataTypeFloat64, DataTypeUInt8, DataTypeUInt16, DataTypeUInt32, DataTypeUInt64,
                               DataTypeInt8, DataTypeInt16, DataTypeInt32, DataTypeInt64>;
        ColumnPtr result = nullptr;
        bool valid = castTypeToEither(Types{}, arguments[0].type.get(), [&](const auto & left_)
        {
            return castTypeToEither(Types{}, arguments[1].type.get(), [&](const auto & right_)
            {
                using R = typename std::decay_t<decltype(right_)>::FieldType;
                const ColumnVector<R> * const_col = checkAndGetColumnConstData<ColumnVector<R>>(arguments[1].column.get());
                if (const_col && const_col->getElement(0) == 0)
                {
                    using L = typename std::decay_t<decltype(left_)>::FieldType;
                    using T = typename NumberTraits::ResultOfFloatingPointDivision<L, R>::Type;
                    auto data_col = ColumnVector<T>::create(arguments[0].column->size(), 0);
                    auto null_map_col = ColumnVector<UInt8>::create(arguments[0].column->size(), 1);
                    result = ColumnNullable::create(std::move(data_col), std::move(null_map_col));
                }
                else
                    result = FunctionBinaryArithmeticWithConstants<Op, Name>::executeImpl(arguments, result_type, input_rows_count);
                return true;
            });
        });
        if (!valid)
            throw Exception(DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {}'s arguments type is not valid", Name::name);
        return result;
    }
};

template <template <typename, typename> class Op, typename Name>
class SparkBinaryArithmeticOverloadResolver : public BinaryArithmeticOverloadResolver<Op, Name>
{
public:
    static FunctionOverloadResolverPtr create(ContextPtr context)
    {
        return std::make_unique<SparkBinaryArithmeticOverloadResolver>(context);
    }
    explicit SparkBinaryArithmeticOverloadResolver(ContextPtr context_) :
        BinaryArithmeticOverloadResolver<Op, Name>(context_),
        context(context_) {}

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
    {
        if (arguments.size() == 2
            && ((arguments[0].column && isColumnConst(*arguments[0].column))
                || (arguments[1].column && isColumnConst(*arguments[1].column))))
        {
            auto function = std::make_shared<SparkFunctionDivideWithConstants<Op, Name>>(
                    arguments[0], arguments[1], return_type, context);
            return std::make_unique<FunctionToFunctionBaseAdaptor>(function, collections::map<DataTypes>(arguments, [](const auto & elem) { return elem.type; }),
                return_type);
        }
        auto function = std::make_shared<SparkFunctionDivide<Op, Name>>(context);
        return std::make_unique<FunctionToFunctionBaseAdaptor>(function, collections::map<DataTypes>(arguments, [](const auto & elem) { return elem.type;}),
            return_type);
    }
    
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return makeNullable(BinaryArithmeticOverloadResolver<Op, Name>::getReturnTypeImpl(arguments));
    }
private:
    ContextPtr context;
};
}
