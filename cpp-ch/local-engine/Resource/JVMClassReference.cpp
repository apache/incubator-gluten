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
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <iostream>

namespace DB::ErrorCodes
{
extern const int UNRECOGNIZED_ARGUMENTS;
}

namespace local_engine
{

JVMClassReference::JVMClassReference(const std::string & class_name_, const std::vector<std::pair<std::string, std::string>> & methods_)
    : class_name(class_name_)
    , methods(methods_)
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
    jvm_class = local_engine::CreateGlobalClassReference(env, class_name.c_str());
    if (jvm_class == nullptr)
        throw DB::Exception(DB::ErrorCodes::UNRECOGNIZED_ARGUMENTS, "Unrecognized class: {}", class_name);
    for (const auto & [method_name, method_sig] : methods)
    {
        jmethodID method_id = local_engine::GetMethodID(env, jvm_class, method_name.c_str(), method_sig.c_str());
        if (method_id == nullptr)
            throw DB::Exception(DB::ErrorCodes::UNRECOGNIZED_ARGUMENTS, "Unrecognized method: {}", method_name);
        method_ids[method_name] = method_id;
    }
}

void JVMClassReference::destroy(JNIEnv * env)
{
    env->DeleteGlobalRef(jvm_class);
}

class JVMClassReferenceCreator
{
public:
    JVMClassReferenceCreator(
        const std::string id_, const std::string & class_name_, const std::vector<std::string> & methods_)
    {
        std::vector<std::pair<std::string, std::string>> sig_methods;
        for (size_t i = 0; i + 1 < methods_.size(); i += 2)
            sig_methods.emplace_back(methods_[i], methods_[i + 1]);
        DependencyResourceMetadata metadata{
            .id = id_,
            .creator = [class_name_, sig_methods]() { return std::make_shared<JVMClassReference>(class_name_, sig_methods); },
            .group_id = "jvm_class_reference",
            .description = "JVM class reference: " + class_name_};
        JNIEnvResourceManager::instance().registerResource(metadata);
    }
};

void buildVector(std::vector<std::string>& vec) {
}

template<typename T, typename... Args>
void buildVector(std::vector<std::string>& vec, T&& first, Args&&... args) {
    vec.push_back(std::forward<T>(first));
    buildVector(vec, std::forward<Args>(args)...);
}

template<typename... Args>
std::vector<std::string> createVector(Args&&... args) {
    std::vector<std::string> vec;
    buildVector(vec, std::forward<Args>(args)...);
    return vec;
}

#define STRING_VECTOR(...) createVector(__VA_ARGS__)

// Third part arguments are pairs of method name and method signature
#define REGISTER_JVM_CLASS_REFERENCE(id, class_name, ...) static JVMClassReferenceCreator id##_creator(#id, class_name, createVector(__VA_ARGS__));

REGISTER_JVM_CLASS_REFERENCE(block_stripes_class, "Lorg/apache/spark/sql/execution/datasources/BlockStripes;", "<init>", "(J[J[II)V")
REGISTER_JVM_CLASS_REFERENCE(split_result_class, "Lorg/apache/gluten/vectorized/CHSplitResult;",  "<init>", "(JJJJJJ[J[JJJJJJJ)V")
REGISTER_JVM_CLASS_REFERENCE(block_stats_class, "Lorg/apache/gluten/vectorized/BlockStats;", "<init>", "(JZ)V")
REGISTER_JVM_CLASS_REFERENCE(shuffle_input_stream, "Lorg/apache/gluten/vectorized/ShuffleInputStream;", "read", "(JJ)J")

// Used in NativeSplitter
REGISTER_JVM_CLASS_REFERENCE(splitter_iterator_class, "Lorg/apache/gluten/vectorized/IteratorWrapper;", "hasNext", "()Z", "next", "()J")

// Used in WriteBufferFromJavaOutputStream
REGISTER_JVM_CLASS_REFERENCE(write_buffer_from_java_output_stream_class, "Ljava/io/OutputStream;", "write", "([BII)V", "flush", "()V")

// Used in SourceFromJavaIter
REGISTER_JVM_CLASS_REFERENCE(source_from_java_iterator_class, "Lorg/apache/gluten/execution/ColumnarNativeIterator;", "hasNext", "()Z", "next", "()[B")

// Used in SparkRowToCHColumn
REGISTER_JVM_CLASS_REFERENCE(row_to_column_iterator_class, "Lorg/apache/gluten/execution/SparkRowIterator;", "hasNext", "()Z", "next", "()[B", "nextBatch", "()Ljava/nio/ByteBuffer;")

}