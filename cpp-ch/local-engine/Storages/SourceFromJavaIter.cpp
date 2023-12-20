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
#include <Columns/ColumnNullable.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <jni/SparkBackendSettings.h>
#include <jni/jni_common.h>
#include <Common/CHUtil.h>
#include <Common/DebugUtils.h>
#include <Common/Exception.h>
#include <Common/JNIUtils.h>


namespace local_engine
{
jclass SourceFromJavaIter::serialized_record_batch_iterator_class = nullptr;
jmethodID SourceFromJavaIter::serialized_record_batch_iterator_hasNext = nullptr;
jmethodID SourceFromJavaIter::serialized_record_batch_iterator_next = nullptr;


static DB::Block getRealHeader(const DB::Block & header)
{
    if (header.columns())
        return header;
    return BlockUtil::buildRowCountHeader();
}
SourceFromJavaIter::SourceFromJavaIter(DB::ContextPtr context_, DB::Block header, jobject java_iter_, bool materialize_input_)
    : DB::ISource(getRealHeader(header))
    , java_iter(java_iter_)
    , materialize_input(materialize_input_)
    , original_header(header)
    , context(context_)
{
    max_concatenate_rows = SparkBackendSettings::getRuntimeLongConf(
        SparkBackendSettings::max_source_concatenate_rows_key, SparkBackendSettings::default_max_source_concatenate_rows);
    max_concatenate_bytes = SparkBackendSettings::getRuntimeLongConf(
        SparkBackendSettings::max_source_concatenate_bytes_key, SparkBackendSettings::default_max_source_concatenate_bytes);
}
DB::Chunk SourceFromJavaIter::generate()
{
    GET_JNIENV(env)
    size_t max_block_size = context->getSettingsRef().max_block_size;
    DB::Chunk result;
    size_t buffer_rows = 0;
    size_t buffer_bytes = 0;
    std::vector<DB::Block> blocks;
    if (pending_block.rows())
    {
        buffer_rows += pending_block.rows();
        buffer_bytes += pending_block.bytes();
        blocks.emplace_back(std::move(pending_block));
        pending_block = {};
    }

    while (!buffer_rows || (buffer_rows < max_concatenate_rows && buffer_bytes < max_concatenate_bytes))
    {
        jboolean has_next = safeCallBooleanMethod(env, java_iter, serialized_record_batch_iterator_hasNext);
        if (!has_next)
            break;
        jbyteArray block_address = static_cast<jbyteArray>(safeCallObjectMethod(env, java_iter, serialized_record_batch_iterator_next));
        DB::Block * block = reinterpret_cast<DB::Block *>(byteArrayToLong(env, block_address));

        if (!blocks.empty() && (blocks[0].info.is_overflows != block->info.is_overflows || blocks[0].info.bucket_num != block->info.bucket_num))
        {
            pending_block = std::move(*block);
            break;
        }

        if (block->rows())
        {
            buffer_rows += block->rows();
            buffer_bytes += block->bytes();
            blocks.emplace_back(std::move(*block));
        }
    }

    if (buffer_rows)
    {
        if (original_header.columns())
        {
            auto is_overflows = blocks[0].info.is_overflows;
            auto bucket_num = blocks[0].info.bucket_num;
            auto merged_block = DB::concatenateBlocks(blocks);
            if (materialize_input)
                materializeBlockInplace(merged_block);
            result.setColumns(merged_block.getColumns(), buffer_rows);
            convertNullable(result);
            auto info = std::make_shared<DB::AggregatedChunkInfo>();
            info->is_overflows = is_overflows;
            info->bucket_num = bucket_num;
            result.setChunkInfo(info);
        }
        else
        {
            result = BlockUtil::buildRowCountChunk(buffer_rows);
        }
    }

    CLEAN_JNIENV
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
void SourceFromJavaIter::convertNullable(DB::Chunk & chunk)
{
    auto output = this->getOutputs().front().getHeader();
    auto rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();
    for (size_t i = 0; i < columns.size(); ++i)
    {
        DB::WhichDataType which(columns.at(i)->getDataType());
        if (output.getByPosition(i).type->isNullable() && !which.isNullable() && !which.isAggregateFunction())
        {
            columns[i] = DB::makeNullable(columns.at(i));
        }
    }
    chunk.setColumns(columns, rows);
}
}
