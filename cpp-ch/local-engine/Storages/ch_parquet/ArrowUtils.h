#pragma once
#include <arrow/type_fwd.h>
#include <parquet/level_conversion.h>
#include <Common/Exception.h>
namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}
}

namespace local_engine
{

/// \brief Execute an expression that returns a Result, extracting its value
/// into the variable defined by `lhs` (or throwing a Status on error).

template <typename T>
T throw_or_return_result(::arrow::Result<T> && result)
{
    if (result.ok())
        return std::move(result).ValueUnsafe();
    else
        throw DB::Exception::createRuntime(DB::ErrorCodes::BAD_ARGUMENTS, result.status().ToString());
}
#define THROW_ARROW_NOT_OK_OR_ASSIGN(lhs, rexpr) \
    lhs \
    { \
        throw_or_return_result((rexpr)) \
    }

#define THROW_ARROW_NOT_OK(status) \
    do \
    { \
        if (::arrow::Status _s = (status); !_s.ok()) \
            throw DB::Exception::createRuntime(DB::ErrorCodes::BAD_ARGUMENTS, _s.ToString()); \
    } while (false)

parquet::internal::LevelInfo ComputeLevelInfo(const parquet::ColumnDescriptor * descr);

/// \brief Get Arrow default memory pool.
inline arrow::MemoryPool * default_arrow_pool()
{
    return arrow::default_memory_pool();
}

}
