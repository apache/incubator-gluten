#pragma once

#include <memory>
#include <string>

// Forward Declarations
struct JNIEnv_;
using JNIEnv = JNIEnv_;

namespace local_engine
{
class StorageJoinWrapper;

class BroadcastBuildSideCache
{
public:
    static void init(JNIEnv *);
    static void destroy(JNIEnv *);

    static std::shared_ptr<StorageJoinWrapper> get(const std::string & id);
};
}
