#pragma once
#include <Shuffle/ShuffleReader.h>
#include <Storages/StorageJoinFromReadBuffer.h>

namespace local_engine
{
class StorageJoinWrapper
{
public:
    explicit StorageJoinWrapper() = default;
    explicit StorageJoinWrapper(std::shared_ptr<StorageJoinFromReadBuffer> & storage_join_) : storage_join(std::move(storage_join_)){};

    void build(
        const std::string & key,
        jobject input,
        size_t io_buffer_size,
        const DB::Names & key_names_,
        DB::JoinKind kind_,
        DB::JoinStrictness strictness_,
        const DB::ColumnsDescription & columns_);

    std::shared_ptr<StorageJoinFromReadBuffer> getStorage() { return storage_join; };

private:
    std::shared_ptr<StorageJoinFromReadBuffer> storage_join;
    std::mutex build_lock_mutex;
};


class BroadCastJoinBuilder
{
public:
    static std::shared_ptr<StorageJoinWrapper> buildJoin(
        const std::string & key,
        jobject input,
        size_t io_buffer_size,
        const DB::Names & key_names_,
        DB::JoinKind kind_,
        DB::JoinStrictness strictness_,
        const DB::ColumnsDescription & columns_);

    static std::shared_ptr<StorageJoinWrapper> buildJoin(
        const std::string & key,
        jobject input,
        size_t io_buffer_size,
        const std::string & join_keys,
        const std::string & join_type,
        const std::string & named_struct);

    static void cleanBuildHashTable(const std::string & hash_table_id, jlong instance);

    static size_t cachedHashTableCount() { return storage_join_map.size(); };

    static std::shared_ptr<StorageJoinFromReadBuffer> getJoin(const std::string & key);

    static void clean();

private:
    static std::unordered_map<std::string, std::shared_ptr<StorageJoinWrapper>> storage_join_map;
    static std::mutex join_lock_mutex;
};


}
