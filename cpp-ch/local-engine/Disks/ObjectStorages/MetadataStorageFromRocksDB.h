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
#include <config.h>
#if USE_ROCKSDB
#include <Disks/DiskLocal.h>
#include <Disks/ObjectStorages/DiskObjectStorageMetadata.h>
#include <Disks/ObjectStorages/IMetadataStorage.h>
#include <Disks/ObjectStorages/MetadataOperationsHolder.h>
#include <rocksdb/db.h>

namespace local_engine
{
class MetadataStorageFromRocksDB final : public DB::IMetadataStorage
{
    friend class MetadataStorageFromRocksDBTransaction;

public:
    static DB::MetadataStoragePtr create(
        const std::string & name,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        DB::ObjectStoragePtr object_storage);
    MetadataStorageFromRocksDB(const String & compatible_key_prefix, const String & rocksdb_dir, DB::ObjectStoragePtr & object_storage, size_t metadata_clean_task_interval_seconds);
    DB::MetadataTransactionPtr createTransaction() override;
    const std::string & getPath() const override;
    DB::MetadataStorageType getType() const override;
    bool existsFile(const std::string & path) const override;
    bool existsDirectory(const std::string & path) const override;
    bool existsFileOrDirectory(const std::string & path) const override;
    uint64_t getFileSize(const std::string & path) const override;
    Poco::Timestamp getLastModified(const std::string & path) const override;
    bool supportsChmod() const override;
    bool supportsStat() const override;
    std::vector<std::string> listDirectory(const std::string & path) const override;
    DB::DirectoryIteratorPtr iterateDirectory(const std::string & path) const override;
    uint32_t getHardlinkCount(const std::string & path) const override;
    DB::StoredObjects getStorageObjects(const std::string & path) const override;
    DB::DiskObjectStorageMetadataPtr readMetadata(const std::string & path) const;
    DB::DiskObjectStorageMetadataPtr readMetadataUnlocked(const std::string & path, std::unique_lock<DB::SharedMutex> & lock) const;
    DB::DiskObjectStorageMetadataPtr readMetadataUnlocked(const std::string & path, std::shared_lock<DB::SharedMutex> & lock) const;
    std::string readFileToString(const std::string & path) const override;
    void shutdown() override;
    void cleanOutdatedMetadataThreadFunc();

private:
    DB::SharedMutex & getMetadataMutex() const;
    rocksdb::DB & getRocksDB() const;

    using RocksDBPtr = rocksdb::DB *;
    RocksDBPtr rocksdb = nullptr;
    mutable DB::SharedMutex metadata_mutex;
    String compatible_key_prefix;
    String rocksdb_dir;
    DB::ObjectStoragePtr object_storage;
    size_t metadata_clean_task_interval_seconds;
    DB::BackgroundSchedulePool::TaskHolder metadata_clean_task;
    LoggerPtr logger;
};

class MetadataStorageFromRocksDBTransaction final : public DB::IMetadataTransaction, private DB::MetadataOperationsHolder
{
public:
    MetadataStorageFromRocksDBTransaction(const MetadataStorageFromRocksDB & metadata_storage_) : metadata_storage(metadata_storage_) { }

    void commit() override;
    const DB::IMetadataStorage & getStorageForNonTransactionalReads() const override;
    bool supportsChmod() const override;
    void createEmptyMetadataFile(const std::string & path) override;
    void createMetadataFile(const std::string & path, DB::ObjectStorageKey key, uint64_t size_in_bytes) override;

    void writeStringToFile(const std::string &, const std::string &) override;
    void createDirectory(const std::string &) override;
    void createDirectoryRecursive(const std::string &) override;
    void removeDirectory(const std::string &) override;
    void removeRecursive(const std::string &) override;
    void unlinkFile(const std::string &) override;

private:
    const MetadataStorageFromRocksDB & metadata_storage;
};
}
#endif
