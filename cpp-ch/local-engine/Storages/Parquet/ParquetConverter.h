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
#include <Core/Field.h>
#include <parquet/statistics.h>
#include <parquet/types.h>
#include <Common/PODArray.h>

namespace local_engine
{
template <typename PhysicalType>
auto parquetCast(const DB::Field & value) -> typename PhysicalType::c_type
{
    using T = typename PhysicalType::c_type;
    if constexpr (std::is_same_v<PhysicalType, parquet::Int32Type>)
        return static_cast<T>(value.get<Int64>());
    else if constexpr (std::is_same_v<PhysicalType, parquet::ByteArrayType>)
    {
        assert(value.getType() == DB::Field::Types::String);
        const std::string & s = value.get<std::string>();
        const auto * const ptr = reinterpret_cast<const uint8_t *>(s.data());
        return parquet::ByteArray(static_cast<uint32_t>(s.size()), ptr);
    }
    else if constexpr (std::is_same_v<PhysicalType, parquet::FLBAType>)
    {
        abort();
    }
    else
        return value.get<T>(); // FLOAT, DOUBLE, INT64
}

// Int32 Int64 Float Double
template <typename DType, typename Col>
struct ConverterNumeric
{
    using From = typename Col::Container::value_type;
    using To = typename DType::c_type;

    const Col & column;
    DB::PODArray<To> buf;

    explicit ConverterNumeric(const DB::ColumnPtr & c) : column(assert_cast<const Col &>(*c)) { }

    const To * getBatch(size_t offset, size_t count)
    {
        if constexpr (sizeof(*column.getData().data()) == sizeof(To))
            return reinterpret_cast<const To *>(column.getData().data() + offset);
        else
        {
            buf.resize(count);
            for (size_t i = 0; i < count; ++i)
                buf[i] = static_cast<To>(column.getData()[offset + i]); // NOLINT
            return buf.data();
        }
    }
};

using ConverterInt32_8 = ConverterNumeric<parquet::Int32Type, DB::ColumnVector<Int8>>;
using ConverterInt32_16 = ConverterNumeric<parquet::Int32Type, DB::ColumnVector<Int16>>;
using ConverterInt32 = ConverterNumeric<parquet::Int32Type, DB::ColumnVector<Int32>>;
using ConverterInt32_u8 = ConverterNumeric<parquet::Int32Type, DB::ColumnVector<UInt8>>;
using ConverterInt32_u16 = ConverterNumeric<parquet::Int32Type, DB::ColumnVector<UInt16>>;
using ConverterInt32_u = ConverterNumeric<parquet::Int32Type, DB::ColumnVector<UInt32>>;

using ConverterInt64 = ConverterNumeric<parquet::Int64Type, DB::ColumnVector<Int64>>;
using ConverterInt64_u = ConverterNumeric<parquet::Int64Type, DB::ColumnVector<UInt64>>;

using ConverterDouble = ConverterNumeric<parquet::DoubleType, DB::ColumnVector<Float64>>;

struct ConverterString
{
    const DB::ColumnString & column;
    DB::PODArray<parquet::ByteArray> buf;

    explicit ConverterString(const DB::ColumnPtr & c) : column(assert_cast<const DB::ColumnString &>(*c)) { }

    const parquet::ByteArray * getBatch(size_t offset, size_t count)
    {
        buf.resize(count);
        for (size_t i = 0; i < count; ++i)
        {
            StringRef s = column.getDataAt(offset + i);
            buf[i] = parquet::ByteArray(static_cast<UInt32>(s.size), reinterpret_cast<const uint8_t *>(s.data));
        }
        return buf.data();
    }
};


class BaseConverter
{
public:
    virtual ~BaseConverter() = default;
};

template <typename DType>
class ParquetConverter : public BaseConverter
{
protected:
    using T = typename DType::c_type;

public:
    virtual const T * getBatch(size_t offset, size_t count) = 0;
    static std::shared_ptr<ParquetConverter<DType>> Make(const DB::ColumnPtr & c);
};

template <typename DType, typename CONVERT>
class ParquetConverterImpl final : public ParquetConverter<DType>
{
public:
    explicit ParquetConverterImpl(CONVERT && converter) : converter_(std::move(converter)) { }
    const typename ParquetConverter<DType>::T * getBatch(size_t offset, size_t count) override
    {
        return converter_.getBatch(offset, count);
    }

private:
    CONVERT converter_;
};


template <typename DType>
std::shared_ptr<ParquetConverter<DType>> ParquetConverter<DType>::Make(const DB::ColumnPtr & c)
{
    std::shared_ptr<BaseConverter> result;

    using namespace DB;
    switch (DType::type_num)
    {
        case parquet::Type::BOOLEAN:
            break;
        case parquet::Type::INT32:
            switch (c->getDataType())
            {
                case TypeIndex::Int8:
                    result = std::make_shared<ParquetConverterImpl<parquet::Int32Type, ConverterInt32_8>>(ConverterInt32_8(c));
                    break;
                case TypeIndex::Int16:
                    result = std::make_shared<ParquetConverterImpl<parquet::Int32Type, ConverterInt32_16>>(ConverterInt32_16(c));
                    break;
                case TypeIndex::Int32:
                    result = std::make_shared<ParquetConverterImpl<parquet::Int32Type, ConverterInt32>>(ConverterInt32(c));
                    break;
                case TypeIndex::UInt8:
                    result = std::make_shared<ParquetConverterImpl<parquet::Int32Type, ConverterInt32_u8>>(ConverterInt32_u8(c));
                    break;
                case TypeIndex::UInt16:
                    result = std::make_shared<ParquetConverterImpl<parquet::Int32Type, ConverterInt32_u16>>(ConverterInt32_u16(c));
                    break;
                case TypeIndex::UInt32:
                    result = std::make_shared<ParquetConverterImpl<parquet::Int32Type, ConverterInt32_u>>(ConverterInt32_u(c));
                    break;
                default:
                    break;
            }
        case parquet::Type::INT64:
            switch (c->getDataType())
            {
                case TypeIndex::Int64:
                    result = std::make_shared<ParquetConverterImpl<parquet::Int64Type, ConverterInt64>>(ConverterInt64(c));
                    break;
                case TypeIndex::UInt64:
                    result = std::make_shared<ParquetConverterImpl<parquet::Int64Type, ConverterInt64_u>>(ConverterInt64_u(c));
                    break;
                default:
                    break;
            }
            break;
        case parquet::Type::INT96:
            break;
        case parquet::Type::FLOAT:
            break;
        case parquet::Type::DOUBLE:
            switch (c->getDataType())
            {
                case TypeIndex::Float64:
                    result = std::make_shared<ParquetConverterImpl<parquet::DoubleType, ConverterDouble>>(ConverterDouble(c));
                    break;
                default:
                    break;
            }
            break;
        case parquet::Type::BYTE_ARRAY:
            switch (c->getDataType())
            {
                case TypeIndex::String:
                    result = std::make_shared<ParquetConverterImpl<parquet::ByteArrayType, ConverterString>>(ConverterString(c));
                    break;
                default:
                    break;
            }
            break;
        default:
            break;
    }
    assert(result);
    return std::static_pointer_cast<ParquetConverter<DType>>(result);
}

}
