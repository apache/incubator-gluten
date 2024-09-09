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
#include "SourceFromJavaIter.h"
#include <Interpreters/castColumn.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <jni/jni_common.h>
#include <Common/CHUtil.h>
#include <Common/Exception.h>
#include <Common/JNIUtils.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}

namespace local_engine
{
jclass SourceFromJavaIter::serialized_record_batch_iterator_class = nullptr;
jmethodID SourceFromJavaIter::serialized_record_batch_iterator_hasNext = nullptr;
jmethodID SourceFromJavaIter::serialized_record_batch_iterator_next = nullptr;

static DB::Block getRealHeader(const DB::Block & header, const std::optional<DB::Block> & first_block)
{
    if (!header)
        return BlockUtil::buildRowCountHeader();
    if (!first_block.has_value())
        return header;
    if (header.columns() != first_block.value().columns())
        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR,
            "Header first block have different number of columns, header:{} first_block:{}",
            header.dumpStructure(),
            first_block.value().dumpStructure());

    DB::Block result;
    const size_t column_size = header.columns();
    for (size_t i = 0; i < column_size; ++i)
    {
        const auto & header_column = header.getByPosition(i);
        const auto & input_column = first_block.value().getByPosition(i);
        chassert(header_column.name == input_column.name);

        DB::WhichDataType input_which(input_column.type);
        /// Some AggregateFunctions may have parameters, so we need to use the exact type from the first block.
        /// e.g. spark approx_percentile -> CH quantilesGK(accuracy, level1, level2, ...), the intermediate result type
        /// parsed from substrait plan is always AggregateFunction(10000, 1)(quantilesGK, arg_type), which maybe different
        /// from the actual intermediate result type from input block. So we need to use the exact type from the input block.
        auto type = input_which.isAggregateFunction() ? input_column.type : header_column.type;
        result.insert(DB::ColumnWithTypeAndName(type, header_column.name));
    }
    return result;
}


std::optional<DB::Block> SourceFromJavaIter::peekBlock(JNIEnv * env, jobject java_iter)
{
    jboolean has_next = safeCallBooleanMethod(env, java_iter, serialized_record_batch_iterator_hasNext);
    if (!has_next)
        return std::nullopt;

    jbyteArray block_addr = static_cast<jbyteArray>(safeCallObjectMethod(env, java_iter, serialized_record_batch_iterator_next));
    auto * block = reinterpret_cast<DB::Block *>(byteArrayToLong(env, block_addr));
    if (block->columns())
        return std::optional(DB::Block(block->getColumnsWithTypeAndName()));
    else
        return std::nullopt;

}


SourceFromJavaIter::SourceFromJavaIter(
    DB::ContextPtr context_, const DB::Block& header, jobject java_iter_, bool materialize_input_, std::optional<DB::Block> && first_block_)
    : DB::ISource(getRealHeader(header, first_block_))
    , context(context_)
    , original_header(header)
    , java_iter(java_iter_)
    , materialize_input(materialize_input_)
    , first_block(first_block_)
{
}

DB::Chunk SourceFromJavaIter::generate()
{
    if (isCancelled())
        return {};

    GET_JNIENV(env)
    SCOPE_EXIT({CLEAN_JNIENV});

    DB::Block * input_block = nullptr;
    if (first_block.has_value()) [[unlikely]]
    {
        input_block = &first_block.value();
    }
    else if (jboolean has_next = safeCallBooleanMethod(env, java_iter, serialized_record_batch_iterator_hasNext))
    {
        jbyteArray block = static_cast<jbyteArray>(safeCallObjectMethod(env, java_iter, serialized_record_batch_iterator_next));
        input_block = reinterpret_cast<DB::Block *>(byteArrayToLong(env, block));
    }
    else
        return {};

    DB::Chunk result;
    if (original_header)
    {
        const auto & header = getPort().getHeader();
        chassert(header.columns() == input_block->columns());
        /// Cast all input columns in data to expected data types in header
        for (size_t i = 0; i < header.columns(); ++i)
        {
            auto & input_column = input_block->getByPosition(i);
            const auto & expected_type = header.getByPosition(i).type;
            auto column = DB::castColumn(input_column, expected_type);
            input_column.column = column;
            input_column.type = expected_type;
        }

        /// Do materializing after casting is faster than materializing before casting
        if (materialize_input)
            materializeBlockInplace(*input_block);

        auto info = std::make_shared<DB::AggregatedChunkInfo>();
        info->is_overflows = input_block->info.is_overflows;
        info->bucket_num = input_block->info.bucket_num;
        result.getChunkInfos().add(std::move(info));
        result.setColumns(input_block->getColumns(), input_block->rows());
    }
    else
    {
        result = BlockUtil::buildRowCountChunk(input_block->rows());
        auto info = std::make_shared<DB::AggregatedChunkInfo>();
        result.getChunkInfos().add(std::move(info));
    }
    first_block = std::nullopt;
    return result;
}

SourceFromJavaIter::~SourceFromJavaIter()
{
    GET_JNIENV(env)
    env->DeleteGlobalRef(java_iter);
    CLEAN_JNIENV
}

Int64 SourceFromJavaIter::byteArrayToLong(JNIEnv * env, jbyteArray arr)
{
    jsize len = env->GetArrayLength(arr);
    assert(len == sizeof(Int64));
    char * c_arr = new char[len];
    env->GetByteArrayRegion(arr, 0, len, reinterpret_cast<jbyte *>(c_arr));
    std::reverse(c_arr, c_arr + 8);
    Int64 result = reinterpret_cast<Int64 *>(c_arr)[0];
    delete[] c_arr;
    return result;
}

}
