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

#include "GlutenDiskHDFS.h"
#include <ranges>
#include <Disks/ObjectStorages/CompactObjectStorageDiskTransaction.h>
#include <Interpreters/Context.h>
#include <Common/QueryContext.h>
#include <Common/Throttler.h>

#if USE_HDFS

namespace local_engine
{
using namespace DB;

DiskTransactionPtr GlutenDiskHDFS::createTransaction()
{
    return std::make_shared<CompactObjectStorageDiskTransaction>(*this, QueryContext::globalContext()->getTempDataOnDisk());
}

void GlutenDiskHDFS::createDirectory(const String & path)
{
    DiskObjectStorage::createDirectory(path);
    hdfsCreateDirectory(hdfs_object_storage->getHDFSFS(), path.c_str());
}

void GlutenDiskHDFS::createDirectories(const String & path)
{
    DiskObjectStorage::createDirectories(path);
    auto * hdfs = hdfs_object_storage->getHDFSFS();
    fs::path p = "/" + path;
    std::vector<std::string> paths_created;
    while (hdfsExists(hdfs, p.c_str()) < 0)
    {
        paths_created.push_back(p);
        if (!p.has_parent_path())
            break;
        p = p.parent_path();
    }
    for (const auto & path_to_create : paths_created | std::views::reverse)
        hdfsCreateDirectory(hdfs, path_to_create.c_str());
}

void GlutenDiskHDFS::removeDirectory(const String & path)
{
    DiskObjectStorage::removeDirectory(path);
    String abs_path = "/" + path;
    hdfsDelete(hdfs_object_storage->getHDFSFS(), abs_path.c_str(), 1);
}

void GlutenDiskHDFS::removeRecursive(const String & path)
{
    DiskObjectStorage::removeRecursive(path);
    String abs_path = "/" + path;
    hdfsDelete(hdfs_object_storage->getHDFSFS(), abs_path.c_str(), 1);
}

DiskObjectStoragePtr GlutenDiskHDFS::createDiskObjectStorage()
{
    const auto config_prefix = "storage_configuration.disks." + name;
    return std::make_shared<GlutenDiskHDFS>(
        getName(),
        object_key_prefix,
        getMetadataStorage(),
        getObjectStorage(),
        QueryContext::globalContext()->getConfigRef(),
        config_prefix,
        object_storage_creator);
}

std::unique_ptr<DB::WriteBufferFromFileBase> GlutenDiskHDFS::writeFile(
    const String & path,
    size_t buf_size,
    DB::WriteMode mode,
    const DB::WriteSettings & settings)
{
    if (throttler)
        throttler->add(1);
    return DiskObjectStorage::writeFile(path, buf_size, mode, settings);
}
}
#endif