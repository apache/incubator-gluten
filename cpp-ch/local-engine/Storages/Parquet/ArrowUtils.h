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
#include <arrow/type_fwd.h>
#include <parquet/level_conversion.h>
#include <Common/Exception.h>
namespace DB
{
namespace ErrorCodes
{
extern const int INCORRECT_DATA;
extern const int CANNOT_ALLOCATE_MEMORY;
}
}

namespace local_engine
{

/// \brief Execute an expression that returns a Result, extracting its value
/// into the variable defined by `lhs` (or throwing a Status on error).

template <typename T>
T throwORReturnResult(::arrow::Result<T> && result)
{
    if (result.ok())
        return std::move(result).ValueUnsafe();

    throw DB::Exception::createDeprecated(
        result.status().ToString(),
        result.status().IsOutOfMemory() ? DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY : DB::ErrorCodes::INCORRECT_DATA);
}

#define THROW_ARROW_NOT_OK_OR_ASSIGN(lhs, rexpr) \
    lhs \
    { \
        local_engine::throwORReturnResult((rexpr)) \
    }

#define THROW_ARROW_NOT_OK(status) \
    do \
    { \
        if (::arrow::Status _s = (status); !_s.ok()) \
        { \
            throw DB::Exception::createDeprecated( \
                _s.ToString(), _s.IsOutOfMemory() ? DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY : DB::ErrorCodes::INCORRECT_DATA); \
        } \
    } while (false)

parquet::internal::LevelInfo computeLevelInfo(const parquet::ColumnDescriptor * descr);

/// \brief Get Arrow default memory pool.
inline arrow::MemoryPool * defaultArrowPool()
{
    return arrow::default_memory_pool();
}

}
