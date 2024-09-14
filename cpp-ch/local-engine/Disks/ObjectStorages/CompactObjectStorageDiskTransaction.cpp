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

#include "CompactObjectStorageDiskTransaction.h"

#include <format>
#include <ranges>

namespace local_engine
{

bool isMetaDataFile(const std::string & path)
{
    return !path.ends_with("bin");
}

void CompactObjectStorageDiskTransaction::commit()
{
    auto metadata_tx = disk.getMetadataStorage()->createTransaction();
    std::filesystem::path data_path = std::filesystem::path(prefix_path) / "data.bin";
    std::filesystem::path meta_path = std::filesystem::path(prefix_path) / "meta.bin";

    auto object_storage = disk.getObjectStorage();
    auto data_key = object_storage->generateObjectKeyForPath(data_path, std::nullopt);
    auto meta_key = object_storage->generateObjectKeyForPath(meta_path, std::nullopt);

    disk.createDirectories(prefix_path);
    auto data_write_buffer = object_storage->writeObject(DB::StoredObject(data_key.serialize(), data_path), DB::WriteMode::Rewrite);
    auto meta_write_buffer = object_storage->writeObject(DB::StoredObject(meta_key.serialize(), meta_path), DB::WriteMode::Rewrite);
    String buffer;
    buffer.resize(1024 * 1024);

    auto merge_files = [&](std::ranges::input_range auto && list, DB::WriteBuffer & out, const DB::ObjectStorageKey & key , const String &local_path)
    {
        size_t offset = 0;
        std::ranges::for_each(
            list,
            [&](auto & item)
            {
                DB::DiskObjectStorageMetadata metadata(object_storage->getCommonKeyPrefix(), item.first);
                DB::ReadBufferFromFilePRead read(item.second->getAbsolutePath());
                int file_size = 0;
                while (int count = read.readBig(buffer.data(), buffer.size()))
                {
                    file_size += count;
                    out.write(buffer.data(), count);
                }
                metadata.addObject(key, offset, file_size);
                metadata_tx->writeStringToFile(item.first, metadata.serializeToString());
                offset += file_size;
            });

        // You can load the complete file in advance through this metadata original, which improves the download efficiency of mergetree metadata.
        DB::DiskObjectStorageMetadata whole_meta(object_storage->getCommonKeyPrefix(), local_path);
        whole_meta.addObject(key, 0, offset);
        metadata_tx->writeStringToFile(local_path, whole_meta.serializeToString());
        out.sync();
        out.finalize();
    };

    merge_files(files | std::ranges::views::filter([](auto file) { return !isMetaDataFile(file.first); }), *data_write_buffer, data_key, data_path);
    merge_files(files | std::ranges::views::filter([](auto file) { return isMetaDataFile(file.first); }), *meta_write_buffer, meta_key, meta_path);

    metadata_tx->commit();
    files.clear();
}

std::unique_ptr<DB::WriteBufferFromFileBase> CompactObjectStorageDiskTransaction::writeFile(
    const std::string & path,
    size_t buf_size,
    DB::WriteMode mode,
    const DB::WriteSettings &,
    bool)
{
    if (mode != DB::WriteMode::Rewrite)
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Operation `writeFile` with Append is not implemented");
    }
    if (prefix_path.empty())
        prefix_path = path.substr(0, path.find_last_of('/'));
    else if (!path.starts_with(prefix_path))
        throw DB::Exception(
            DB::ErrorCodes::NOT_IMPLEMENTED,
            "Don't support write file in different dirs, path {}, prefix path: {}",
            path,
            prefix_path);
    auto tmp = std::make_shared<DB::TemporaryFileOnDisk>(tmp_data);
    files.emplace_back(path, tmp);
    auto tx = disk.getMetadataStorage()->createTransaction();
    tx->createDirectoryRecursive(std::filesystem::path(path).parent_path());
    tx->createEmptyMetadataFile(path);
    tx->commit();
    return std::make_unique<DB::WriteBufferFromFile>(tmp->getAbsolutePath(), buf_size);
}
}