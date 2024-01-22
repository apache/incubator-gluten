#include "config.h"
#include <Disks/ObjectStorages/ObjectStorageFactory.h>
#if USE_AWS_S3
#include <Disks/ObjectStorages/S3/S3ObjectStorage.h>
#include <Disks/ObjectStorages/S3/diskSettings.h>
#include <Disks/ObjectStorages/S3/DiskS3Utils.h>
#endif

#include <Interpreters/Context.h>
#include <Common/Macros.h>


namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
}
}

namespace local_engine
{
using namespace DB;

#if USE_AWS_S3
static S3::URI getS3URI(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    const ContextPtr & context)
{
    String endpoint = context->getMacros()->expand(config.getString(config_prefix + ".endpoint"));
    S3::URI uri(endpoint);

    /// An empty key remains empty.
    if (!uri.key.empty() && !uri.key.ends_with('/'))
        uri.key.push_back('/');

    return uri;
}

void registerGlutenS3ObjectStorage(ObjectStorageFactory & factory)
{
    static constexpr auto disk_type = "s3_gluten";

    factory.registerObjectStorageType(
        disk_type,
        [](
        const std::string & name,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const ContextPtr & context,
        bool skip_access_check) -> ObjectStoragePtr
        {
            auto uri = getS3URI(config, config_prefix, context);
            auto s3_capabilities = getCapabilitiesFromConfig(config, config_prefix);
            auto settings = getSettings(config, config_prefix, context);
            auto client = getClient(config, config_prefix, context, *settings);
            auto key_generator = getKeyGenerator("s3_plain", uri, config, config_prefix);

            auto object_storage = std::make_shared<S3ObjectStorage>(
                std::move(client),
                std::move(settings),
                uri,
                s3_capabilities,
                key_generator,
                name);

            /// NOTE: should we still perform this check for clickhouse-disks?
            if (!skip_access_check)
            {
                /// If `support_batch_delete` is turned on (default), check and possibly switch it off.
                if (s3_capabilities.support_batch_delete && !checkBatchRemove(*object_storage, uri.key))
                {
                    LOG_WARNING(
                        &Poco::Logger::get("S3ObjectStorage"),
                        "Storage for disk {} does not support batch delete operations, "
                        "so `s3_capabilities.support_batch_delete` was automatically turned off during the access check. "
                        "To remove this message set `s3_capabilities.support_batch_delete` for the disk to `false`.",
                        name
                        );
                    object_storage->setCapabilitiesSupportBatchDelete(false);
                }
            }
            return object_storage;
        });
}

#endif
}
