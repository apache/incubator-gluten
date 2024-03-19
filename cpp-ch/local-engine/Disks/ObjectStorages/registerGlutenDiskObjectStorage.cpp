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
#include "config.h"
#include <Disks/ObjectStorages/ObjectStorageFactory.h>
#if USE_AWS_S3
#include <Disks/ObjectStorages/S3/S3ObjectStorage.h>
#include <Disks/ObjectStorages/S3/diskSettings.h>
#include <Disks/ObjectStorages/S3/DiskS3Utils.h>
#endif

#if USE_HDFS
#include <Disks/ObjectStorages/GlutenHDFSObjectStorage.h>
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
        bool /*skip_access_check*/) -> ObjectStoragePtr
        {
            auto uri = getS3URI(config, config_prefix, context);
            auto s3_capabilities = getCapabilitiesFromConfig(config, config_prefix);
            auto settings = getSettings(config, config_prefix, context);
            auto client = getClient(config, config_prefix, context, *settings);
            auto key_generator = createObjectStorageKeysGeneratorAsIsWithPrefix(uri.key);

            auto object_storage = std::make_shared<S3ObjectStorage>(
                std::move(client),
                std::move(settings),
                uri,
                s3_capabilities,
                key_generator,
                name);
            return object_storage;
        });
}

#endif

#if USE_HDFS
void registerGlutenHDFSObjectStorage(ObjectStorageFactory & factory)
{
    factory.registerObjectStorageType(
        "hdfs_gluten",
        [](
            const std::string & /* name */,
            const Poco::Util::AbstractConfiguration & config,
            const std::string & config_prefix,
            const ContextPtr & context,
            bool /* skip_access_check */) -> ObjectStoragePtr
        {
            auto uri = context->getMacros()->expand(config.getString(config_prefix + ".endpoint"));
            checkHDFSURL(uri);
            if (uri.back() != '/')
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "HDFS path must ends with '/', but '{}' doesn't.", uri);

            std::unique_ptr<HDFSObjectStorageSettings> settings = std::make_unique<HDFSObjectStorageSettings>(
                config.getUInt64(config_prefix + ".min_bytes_for_seek", 1024 * 1024),
                config.getInt(config_prefix + ".objects_chunk_size_to_delete", 1000),
                context->getSettingsRef().hdfs_replication
            );
            return std::make_unique<GlutenHDFSObjectStorage>(uri, std::move(settings), config);
        });
}
#endif
}
