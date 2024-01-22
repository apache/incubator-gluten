#include "config.h"
#include <Disks/DiskFactory.h>
#include <Interpreters/Context.h>
#include <Disks/ObjectStorages/DiskObjectStorage.h>
#include <Disks/ObjectStorages/MetadataStorageFactory.h>
#include <Disks/ObjectStorages/ObjectStorageFactory.h>

#include "registerGlutenDisks.h"

namespace local_engine
{
using namespace DB;
#if USE_AWS_S3
void registerS3ObjectStorage(ObjectStorageFactory & factory);
#endif


void registerGlutenDisks(bool global_skip_access_check)
{
    auto & factory = DiskFactory::instance();

    auto creator = [global_skip_access_check](
    const String & name,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    ContextPtr context,
    const DisksMap & /*map*/) -> DiskPtr
    {
        bool skip_access_check = global_skip_access_check || config.getBool(config_prefix + ".skip_access_check", false);
        auto object_storage = ObjectStorageFactory::instance().create(name, config, config_prefix, context, skip_access_check);
        auto metadata_storage = MetadataStorageFactory::instance().create(
            name, config, config_prefix, object_storage, "s3");

        DiskObjectStoragePtr disk = std::make_shared<DiskObjectStorage>(
            name,
            object_storage->getCommonKeyPrefix(),
            fmt::format("Disk_{}({})", toString(object_storage->getDataSourceDescription().type), name),
            std::move(metadata_storage),
            std::move(object_storage),
            config,
            config_prefix);

        disk->startup(context, skip_access_check);
        return disk;
    };

#if USE_AWS_S3
    auto & object_factory = ObjectStorageFactory::instance();
    registerS3ObjectStorage(object_factory);
    factory.registerDiskType("s3_gluten", creator); /// For compatibility
#endif
}
}
