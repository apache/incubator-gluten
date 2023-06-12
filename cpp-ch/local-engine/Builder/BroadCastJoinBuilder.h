#pragma once
#include <Shuffle/ShuffleReader.h>
#include <Storages/StorageJoinFromReadBuffer.h>

// Forward Declarations
struct JNIEnv_;
using JNIEnv = JNIEnv_;

namespace local_engine
{
namespace BroadCastJoinBuilder
{

    std::shared_ptr<StorageJoinFromReadBuffer> buildJoin(
        const std::string & key,
        jobject input,
        size_t io_buffer_size,
        const std::string & join_keys,
        const std::string & join_type,
        const std::string & named_struct);
    void cleanBuildHashTable(const std::string & hash_table_id, jlong instance);
    std::shared_ptr<StorageJoinFromReadBuffer> getJoin(const std::string & hash_table_id);


    void init(JNIEnv *);
    void destroy(JNIEnv *);
}
}
