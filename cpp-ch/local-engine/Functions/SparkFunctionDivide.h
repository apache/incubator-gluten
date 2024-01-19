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
#include <Common/NaNUtils.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeNullable.h>

using namespace DB;

namespace local_engine
{

template<typename T>
static void checkAndSetNullable(const DB::ColumnPtr & src, DB::PaddedPODArray<T> & data, DB::PaddedPODArray<UInt8> & null_map_data)
{
    const DB::ColumnVector<T> * src_vec = assert_cast<const DB::ColumnVector<T> *>(src.get());
    for (size_t i = 0; i < src_vec->size(); ++i)
    {
        T element = src_vec->getElement(i);
        if (isNaN(element) || !isFinite(element))
        {
            data[i] = 0;
            null_map_data[i] = 1;
        }
        else
            data[i] = element;
    }
}

static ColumnPtr checkAndSetNullable(const DB::ColumnPtr & src, const DataTypePtr & result_type)
{
    auto null_map_col = ColumnVector<UInt8>::create(src->size(), 0);
    switch(removeNullable(result_type)->getTypeId())
    {
        case TypeIndex::Float32:
        {
            auto data_col_float32 = ColumnVector<Float32>::create(src->size());
            checkAndSetNullable(src, data_col_float32->getData(), null_map_col->getData());
            return ColumnNullable::create(std::move(data_col_float32), std::move(null_map_col));
        }
        case TypeIndex::Float64:
        {
            auto data_col_float64 = ColumnVector<Float64>::create(src->size());
            checkAndSetNullable(src, data_col_float64->getData(), null_map_col->getData());
            return ColumnNullable::create(std::move(data_col_float64), std::move(null_map_col));
        }
        default:
            return src;
    }
}

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
        ColumnPtr src = FunctionBinaryArithmetic<Op, Name>::executeImpl(arguments, result_type, input_rows_count);
        return checkAndSetNullable(src, result_type);
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
        ColumnPtr src = FunctionBinaryArithmeticWithConstants<Op, Name>::executeImpl(arguments, result_type, input_rows_count);
        return checkAndSetNullable(src, result_type);
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
