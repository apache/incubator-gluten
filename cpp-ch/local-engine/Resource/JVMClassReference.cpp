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
#include "JVMClassReference.h"
#include <algorithm>
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>

namespace DB::ErrorCodes
{
extern const int UNRECOGNIZED_ARGUMENTS;
}

namespace local_engine
{

JVMClassReference::JVMClassReference(const JVMClassDescription & description_)
    : description(description_)
{
}


jmethodID JVMClassReference::getJMethod(const std::string methodId) const
{
    auto it = method_ids.find(methodId);
    if (it == method_ids.end())
        throw DB::Exception(DB::ErrorCodes::UNRECOGNIZED_ARGUMENTS, "Unrecognized method: {}", methodId);
    return it->second;
}

void JVMClassReference::initialize(JNIEnv * env)
{
    const auto & class_signature = description.class_signature;
    const auto & methods = description.methods;
    const auto & static_methods = description.static_methods;
    jvm_class = local_engine::CreateGlobalClassReference(env, class_signature.c_str());
    if (jvm_class == nullptr)
        throw DB::Exception(DB::ErrorCodes::UNRECOGNIZED_ARGUMENTS, "Unrecognized class: {}", class_signature);
    for (const auto & [method_name, method_sig] : methods)
    {
        jmethodID method_id = local_engine::GetMethodID(env, jvm_class, method_name.c_str(), method_sig.c_str());
        if (method_id == nullptr)
            throw DB::Exception(DB::ErrorCodes::UNRECOGNIZED_ARGUMENTS, "Unrecognized method: {}", method_name);
        method_ids[method_name] = method_id;
    }
    for (const auto & [method_name, method_sig] : static_methods)
    {
        jmethodID method_id = local_engine::GetStaticMethodID(env, jvm_class, method_name.c_str(), method_sig.c_str());
        if (method_id == nullptr)
            throw DB::Exception(DB::ErrorCodes::UNRECOGNIZED_ARGUMENTS, "Unrecognized static method: {}", method_name);
        method_ids[method_name] = method_id;
    }
}

void JVMClassReference::destroy(JNIEnv * env)
{
    env->DeleteGlobalRef(jvm_class);
    jvm_class = nullptr;
    method_ids.clear();
}

class JVMClassReferenceRegister
{
public:
    JVMClassReferenceRegister(const std::string & id_, const JVMClassDescription & description)
    {
        DependencyResourceMetadata metadata{
            .id = id_,
            .creator = [description]() { return std::make_shared<JVMClassReference>(description); },
            .group_id = "jvm_class_reference",
            .description = "JVM class reference: " + description.class_signature};
        JNIEnvResourceManager::instance().registerResource(metadata);
    }
};

#define BEGINE_CLASS_DECLARE(id) \
    static JVMClassDescription id##_description{

#define END_CLASS_DECLARE(id) \
    };\
    static JVMClassReferenceRegister id##_register(#id, id##_description);

BEGINE_CLASS_DECLARE(block_stripes_class)
.class_signature = "Lorg/apache/spark/sql/execution/datasources/BlockStripes;",
.methods = {{"<init>", "(J[J[II)V"}}
END_CLASS_DECLARE(block_stripes_class)

BEGINE_CLASS_DECLARE(split_result_class)
.class_signature = "Lorg/apache/gluten/vectorized/CHSplitResult;",
.methods = {{"<init>", "(JJJJJJ[J[JJJJJJJ)V"}}
END_CLASS_DECLARE(split_result_class)

BEGINE_CLASS_DECLARE(block_stats_class)
.class_signature = "Lorg/apache/gluten/vectorized/BlockStats;",
.methods = {{"<init>", "(JZ)V"}}
END_CLASS_DECLARE(block_stats_class)

BEGINE_CLASS_DECLARE(shuffle_input_stream)
.class_signature = "Lorg/apache/gluten/vectorized/ShuffleInputStream;",
.methods = {{"read", "(JJ)J"}}
END_CLASS_DECLARE(shuffle_input_stream)

// Used in NativeSplitter
BEGINE_CLASS_DECLARE(splitter_iterator_class)
.class_signature = "Lorg/apache/gluten/vectorized/IteratorWrapper;",
.methods = {{"hasNext", "()Z"}, {"next", "()J"}}
END_CLASS_DECLARE(splitter_iterator_class)

// Used in WriteBufferFromJavaOutputStream
BEGINE_CLASS_DECLARE(write_buffer_from_java_output_stream_class)
.class_signature = "Ljava/io/OutputStream;",
.methods = {{"write", "([BII)V"}, {"flush", "()V"}}
END_CLASS_DECLARE(write_buffer_from_java_output_stream_class)

// Used in SourceFromJavaIter
BEGINE_CLASS_DECLARE(source_from_java_iterator_class)
.class_signature = "Lorg/apache/gluten/execution/ColumnarNativeIterator;",
.methods = {{"hasNext", "()Z"}, {"next", "()[B"}}
END_CLASS_DECLARE(source_from_java_iterator_class)

// Used in SparkRowToCHColumn
BEGINE_CLASS_DECLARE(row_to_column_iterator_class)
.class_signature = "Lorg/apache/gluten/execution/SparkRowIterator;",
.methods = {{"hasNext", "()Z"}, {"next", "()[B"}, {"nextBatch", "()Ljava/nio/ByteBuffer;"}}
END_CLASS_DECLARE(row_to_column_iterator_class)

BEGINE_CLASS_DECLARE(io_exception_class)
.class_signature = "Ljava/io/IOException;"
END_CLASS_DECLARE(io_exception_class)

BEGINE_CLASS_DECLARE(runtime_exception_class)
.class_signature = "Lorg/apache/gluten/exception/GlutenException;"
END_CLASS_DECLARE(runtime_exception_class)

BEGINE_CLASS_DECLARE(unsupportedoperation_exception_class)
.class_signature = "Ljava/lang/UnsupportedOperationException;"
END_CLASS_DECLARE(unsupportedoperation_exception_class)

BEGINE_CLASS_DECLARE(illegal_access_exception_class)
.class_signature = "Ljava/lang/IllegalAccessException;"
END_CLASS_DECLARE(illegal_access_exception_class)

BEGINE_CLASS_DECLARE(illegal_argument_exception_class)
.class_signature = "Ljava/lang/IllegalArgumentException;"
END_CLASS_DECLARE(illegal_argument_exception_class)

/**
 * Used in BroadCastJoinBuilder
 * Scala object will be compiled into two classes, one is with '$' suffix which is normal class,
 * and one is utility class which only has static method.
 *
 * Here, we use utility class.
 */
BEGINE_CLASS_DECLARE(broadcast_join_builder_side_cache_class)
.class_signature = "Lorg/apache/gluten/execution/CHBroadcastBuildSideCache;",
.static_methods = {{"get", "(Ljava/lang/String;)J"}}
END_CLASS_DECLARE(broadcast_join_builder_side_cache_class)

// Used in CacheManager
BEGINE_CLASS_DECLARE(cache_manager_result_class)
.class_signature = "Lorg/apache/gluten/execution/CacheResult;",
.methods = {{"<init>", "(ILjava/lang/String;)V"}}
END_CLASS_DECLARE(cache_manager_result_class)

// Used in SparkMergeTreeWriterJNI
BEGINE_CLASS_DECLARE(merge_tree_committer_helper)
.class_signature = "Lorg/apache/spark/sql/execution/datasources/v1/clickhouse/MergeTreeCommiterHelper;",
.static_methods = {{"setCurrentTaskWriteInfo", "(Ljava/lang/String;Ljava/lang/String;)V"}}
END_CLASS_DECLARE(merge_tree_committer_helper)

BEGINE_CLASS_DECLARE(spark_row_info_class)
.class_signature = "Lorg/apache/gluten/row/SparkRowInfo;",
.methods = {{"<init>", "([J[JJJJ)V"}}
END_CLASS_DECLARE(spark_row_info_class)

}


