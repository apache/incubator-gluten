#include "BroadCastJoinBuilder.h"
#include <Parser/SerializedPlanParser.h>
#include <Poco/StringTokenizer.h>
#include <Common/CurrentThread.h>
#include <Common/JNIUtils.h>
#include <Common/MemoryTracker.h>
#include <Common/ThreadPool.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE;
}
}

namespace local_engine
{
using namespace DB;

std::unordered_map<std::string, std::shared_ptr<StorageJoinWrapper>> BroadCastJoinBuilder::storage_join_map;
std::mutex BroadCastJoinBuilder::join_lock_mutex;

struct StorageJoinContext
{
    std::string key;
    jobject input;
    size_t io_buffer_size;
    DB::Names key_names;
    DB::JoinKind kind;
    DB::JoinStrictness strictness;
    DB::ColumnsDescription columns;
};

std::shared_ptr<StorageJoinWrapper> BroadCastJoinBuilder::buildJoin(
    const std::string & key,
    jobject input,
    size_t io_buffer_size,
    const DB::Names & key_names_,
    DB::JoinKind kind_,
    DB::JoinStrictness strictness_,
    const DB::ColumnsDescription & columns_)
{
    std::shared_ptr<StorageJoinWrapper> wrapper = nullptr;

    {
        std::lock_guard build_lock(join_lock_mutex);
        if (unlikely(storage_join_map.contains(key)))
        {
            std::shared_ptr<StorageJoinFromReadBuffer> cache = storage_join_map.at(key)->getStorage();
            wrapper = std::make_shared<StorageJoinWrapper>(cache);
        }
        else
            wrapper = std::make_shared<StorageJoinWrapper>();

        storage_join_map.emplace(key, wrapper);
    }

    wrapper->build(key, input, io_buffer_size, key_names_, kind_, strictness_, columns_);
    LOG_DEBUG(&Poco::Logger::get("BroadCastJoinBuilder"), "Broadcast hash table id {} cached.", key);
    return wrapper;
}

void BroadCastJoinBuilder::cleanBuildHashTable(const std::string & hash_table_id, jlong instance)
{
    /// Thread status holds raw pointer on query context, thus it always must be destroyed
    /// It always called by no thread_status. We need create first.
    /// Otherwise global tracker will not free bhj memory.
    DB::ThreadStatus thread_status;

    std::lock_guard release_lock(join_lock_mutex);

    if (storage_join_map.contains(hash_table_id))
    {
        std::shared_ptr<StorageJoinWrapper> cache = storage_join_map.at(hash_table_id);
        if (instance == reinterpret_cast<jlong>(cache.get()))
        {
            storage_join_map.erase(hash_table_id);
            LOG_DEBUG(&Poco::Logger::get("BroadCastJoinBuilder"), "Broadcast hash table id {} removed.", hash_table_id);
        }
    }

    LOG_DEBUG(&Poco::Logger::get("BroadCastJoinBuilder"), "Broadcast hash table size is {}", storage_join_map.size());
}

std::shared_ptr<StorageJoinFromReadBuffer> BroadCastJoinBuilder::getJoin(const std::string & key)
{
    if (storage_join_map.contains(key))
        return storage_join_map.at(key)->getStorage();
    else
        return std::shared_ptr<StorageJoinFromReadBuffer>();
}

std::shared_ptr<StorageJoinWrapper> BroadCastJoinBuilder::buildJoin(
    const std::string & key,
    jobject input,
    size_t io_buffer_size,
    const std::string & join_keys,
    const std::string & join_type,
    const std::string & named_struct)
{
    auto join_key_list = Poco::StringTokenizer(join_keys, ",");
    Names key_names;
    for (const auto & key_name : join_key_list)
    {
        key_names.emplace_back(key_name);
    }
    DB::JoinKind kind;
    DB::JoinStrictness strictness;
    if (join_type == "Inner")
    {
        kind = DB::JoinKind::Inner;
        strictness = DB::JoinStrictness::All;
    }
    else if (join_type == "Semi")
    {
        kind = DB::JoinKind::Left;
        strictness = DB::JoinStrictness::Semi;
    }
    else if (join_type == "Anti")
    {
        kind = DB::JoinKind::Left;
        strictness = DB::JoinStrictness::Anti;
    }
    else if (join_type == "Left")
    {
        kind = DB::JoinKind::Left;
        strictness = DB::JoinStrictness::All;
    }
    else
    {
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "unsupported join type {}.", join_type);
    }

    auto substrait_struct = std::make_unique<substrait::NamedStruct>();
    substrait_struct->ParseFromString(named_struct);

    Block header = SerializedPlanParser::parseNameStruct(*substrait_struct);
    ColumnsDescription columns_description(header.getNamesAndTypesList());
    return buildJoin(key, input, io_buffer_size, key_names, kind, strictness, columns_description);
}

void BroadCastJoinBuilder::clean()
{
    /// It always called by no thread_status. We need create first.
    /// Otherwise global tracker will not free bhj memory.
    DB::ThreadStatus thread_status;
    std::lock_guard release_lock(join_lock_mutex);
    storage_join_map.clear();
}

void StorageJoinWrapper::build(
    const std::string & key,
    jobject input,
    size_t io_buffer_size,
    const DB::Names & key_names_,
    DB::JoinKind kind_,
    DB::JoinStrictness strictness_,
    const DB::ColumnsDescription & columns_)
{
    if (storage_join == nullptr)
    {
        std::lock_guard build_lock(build_lock_mutex);
        if (storage_join == nullptr)
        {
            StorageJoinContext context{key, input, io_buffer_size, key_names_, kind_, strictness_, columns_};
            // use another thread, exclude broadcast memory allocation from current memory tracker
            auto func = [this, &context]() -> void
            {
                try
                {
                    storage_join = std::make_shared<StorageJoinFromReadBuffer>(
                        std::make_unique<ReadBufferFromJavaInputStream>(context.input, context.io_buffer_size),
                        StorageID("default", context.key),
                        context.key_names,
                        true,
                        SizeLimits(),
                        context.kind,
                        context.strictness,
                        context.columns,
                        ConstraintsDescription(),
                        context.key,
                        true);
                    LOG_DEBUG(&Poco::Logger::get("BroadCastJoinBuilder"), "Create broadcast storage join {}.", context.key);
                }
                catch (DB::Exception & e)
                {
                    LOG_ERROR(&Poco::Logger::get("BroadCastJoinBuilder"), "storage join create failed, {}", e.displayText());
                }
            };
            ThreadFromGlobalPool build_thread(func);
            build_thread.join();
        }
    }
}

}
