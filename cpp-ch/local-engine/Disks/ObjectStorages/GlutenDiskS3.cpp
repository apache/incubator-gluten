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

#pragma once


#include "GlutenDiskS3.h"
#include <Disks/ObjectStorages/DiskObjectStorage.h>
#include <Parser/SerializedPlanParser.h>
#include "CompactObjectStorageDiskTransaction.h"

#if USE_AWS_S3
namespace local_engine
{

    DB::DiskTransactionPtr GlutenDiskS3::createTransaction()
    {
        return std::make_shared<CompactObjectStorageDiskTransaction>(*this, SerializedPlanParser::global_context->getTempDataOnDisk()->getVolume()->getDisk());
    }

    std::unique_ptr<ReadBufferFromFileBase> GlutenDiskS3::readFile(
        const String & path,
        const ReadSettings & settings,
        std::optional<size_t> read_hint,
        std::optional<size_t> file_size) const
    {
        ReadSettings copy_settings = settings;
        // Threadpool read is not supported for s3 compact version currently
        copy_settings.remote_fs_method = RemoteFSReadMethod::read;
        return DiskObjectStorage::readFile(path, copy_settings, read_hint, file_size);
    }

    DiskObjectStoragePtr GlutenDiskS3::createDiskObjectStorage()
    {
        const auto config_prefix = "storage_configuration.disks." + name;
        return std::make_shared<GlutenDiskS3>(
            getName(),
            object_key_prefix,
            getMetadataStorage(),
            getObjectStorage(),
            SerializedPlanParser::global_context->getConfigRef(),
            config_prefix,
            object_storage_creator);
    }

}

#endif
