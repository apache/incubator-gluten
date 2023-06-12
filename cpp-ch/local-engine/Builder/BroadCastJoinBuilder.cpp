#include "BroadCastJoinBuilder.h"
#include <jni.h>
#include <Parser/SerializedPlanParser.h>
#include <jni/SharedPointerWrapper.h>
#include <jni/jni_common.h>
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
namespace BroadCastJoinBuilder
{
    static jclass Java_CHBroadcastBuildSideCache = nullptr;
    static jmethodID Java_get = nullptr;
    jlong callJavaGet(const std::string & id)
    {
        GET_JNIENV(env)
        jstring s = charTojstring(env, id.c_str());
        auto result = safeCallStaticLongMethod(env, Java_CHBroadcastBuildSideCache, Java_get, s);
        CLEAN_JNIENV

        return result;
    }

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

    std::shared_ptr<StorageJoinFromReadBuffer> buildInBackground(
        const std::string & key,
        jobject input,
        size_t io_buffer_size,
        const DB::Names & key_names_,
        DB::JoinKind kind_,
        DB::JoinStrictness strictness_,
        const DB::ColumnsDescription & columns_)
    {
        std::shared_ptr<StorageJoinFromReadBuffer> result;
        StorageJoinContext context{key, input, io_buffer_size, key_names_, kind_, strictness_, columns_};
        // use another thread, exclude broadcast memory allocation from current memory tracker
        auto func = [&context, &result]() -> void
        {
            try
            {
                result = std::make_shared<StorageJoinFromReadBuffer>(
                    std::make_unique<ReadBufferFromJavaInputStream>(context.input, context.io_buffer_size),
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
        return result;
    }

    void cleanBuildHashTable(const std::string & hash_table_id, jlong instance)
    {
        /// Thread status holds raw pointer on query context, thus it always must be destroyed
        /// It always called by no thread_status. We need create first.
        /// Otherwise global tracker will not free bhj memory.
        DB::ThreadStatus thread_status;
        SharedPointerWrapper<StorageJoinFromReadBuffer>::dispose(instance);
        LOG_DEBUG(&Poco::Logger::get("BroadCastJoinBuilder"), "Broadcast hash table {} is cleaned", hash_table_id);
    }

    std::shared_ptr<StorageJoinFromReadBuffer> getJoin(const std::string & key)
    {
        jlong result = callJavaGet(key);

        if (unlikely(result == 0))
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "broadcast table {} not found in cache.", key);
        }

        auto wrapper = SharedPointerWrapper<StorageJoinFromReadBuffer>::sharedPtr(result);
        if (unlikely(!wrapper))
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "broadcast table {} not found, cache value is invalidated.", key);
        }

        return wrapper;
    }

    std::shared_ptr<StorageJoinFromReadBuffer> buildJoin(
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
        return buildInBackground(key, input, io_buffer_size, key_names, kind, strictness, columns_description);
    }

    void init(JNIEnv * env)
    {
        /**
     * Scala object will be compiled into two classes, one is with '$' suffix which is normal class,
     * and one is utility class which only has static method.
     *
     * Here, we use utility class.
     */

        const char * classSig = "Lio/glutenproject/execution/CHBroadcastBuildSideCache;";
        Java_CHBroadcastBuildSideCache = CreateGlobalClassReference(env, classSig);
        Java_get = GetStaticMethodID(env, Java_CHBroadcastBuildSideCache, "get", "(Ljava/lang/String;)J");
    }

    void destroy(JNIEnv * env)
    {
        env->DeleteGlobalRef(Java_CHBroadcastBuildSideCache);
    }

}
}
