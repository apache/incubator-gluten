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
SourceFromJavaIter::SourceFromJavaIter(DB::Block header, jobject java_iter_, bool materialize_input_)
    : DB::ISource(getRealHeader(header)), java_iter(java_iter_), materialize_input(materialize_input_), original_header(header)
{
}
DB::Chunk SourceFromJavaIter::generate()
{
    GET_JNIENV(env)
    jboolean has_next = safeCallBooleanMethod(env, java_iter, serialized_record_batch_iterator_hasNext);
    DB::Chunk result;
    if (has_next)
    {
        jbyteArray block = static_cast<jbyteArray>(safeCallObjectMethod(env, java_iter, serialized_record_batch_iterator_next));
        DB::Block * data = reinterpret_cast<DB::Block *>(byteArrayToLong(env, block));
        if(materialize_input)
            materializeBlockInplace(*data);
        if (data->rows() > 0)
        {
            size_t rows = data->rows();
            if (original_header.columns())
            {
                result.setColumns(data->mutateColumns(), rows);
                convertNullable(result);
                auto info = std::make_shared<DB::AggregatedChunkInfo>();
                info->is_overflows = data->info.is_overflows;
                info->bucket_num = data->info.bucket_num;
                result.setChunkInfo(info);
            }
            else
            {
                result = BlockUtil::buildRowCountChunk(rows);
            }
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
