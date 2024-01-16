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
#include <jni.h>
#include <Processors/ISource.h>
#include <Interpreters/Context.h>
#include <Columns/IColumn.h>

namespace local_engine
{
class SourceFromJavaIter : public DB::ISource
{
public:
    static jclass serialized_record_batch_iterator_class;
    static jmethodID serialized_record_batch_iterator_hasNext;
    static jmethodID serialized_record_batch_iterator_next;

    static Int64 byteArrayToLong(JNIEnv * env, jbyteArray arr);

    SourceFromJavaIter(DB::ContextPtr context_, DB::Block header, jobject java_iter_, bool materialize_input_);
    ~SourceFromJavaIter() override;

    String getName() const override { return "SourceFromJavaIter"; }

private:
    DB::Chunk generate() override;
    void convertNullable(DB::Chunk & chunk);
    DB::ColumnPtr convertNestedNullable(const DB::ColumnPtr & column, const DB::DataTypePtr & target_type);

    jobject java_iter;
    bool materialize_input;
    DB::ContextPtr context;
    DB::Block original_header;
};

}
