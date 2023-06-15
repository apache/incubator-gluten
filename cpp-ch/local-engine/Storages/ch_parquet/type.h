#pragma once
#include <generated/parquet_types.h>
#include <base/types.h>

namespace DB
{
using level_t = int16_t;

template <parquet::format::Type::type type>
struct PhysicalTypeTraits {};

template <>
struct PhysicalTypeTraits<parquet::format::Type::BOOLEAN> {
    using CppType = bool;
};

template <>
struct PhysicalTypeTraits<parquet::format::Type::INT32> {
    using CppType = Int32;
};

template <>
struct PhysicalTypeTraits<parquet::format::Type::INT64> {
    using CppType = Int64;
};

template <>
struct PhysicalTypeTraits<parquet::format::Type::INT96> {
    //TODO implement int96
    using CppType = Int64;
};

template <>
struct PhysicalTypeTraits<parquet::format::Type::FLOAT> {
    using CppType = Float32;
};

template <>
struct PhysicalTypeTraits<parquet::format::Type::DOUBLE> {
    using CppType = Float64;
};

template <>
struct PhysicalTypeTraits<parquet::format::Type::BYTE_ARRAY> {
    using CppType = DB::String;
};

//template <>
//struct PhysicalTypeTraits<parquet::format::Type::FIXED_LEN_BYTE_ARRAY> {
//    using CppType = Slice;
//};
}
