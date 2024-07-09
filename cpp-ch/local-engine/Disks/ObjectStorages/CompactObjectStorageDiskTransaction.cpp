#include "CompactObjectStorageDiskTransaction.h"

#include <format>

namespace local_engine
{

void CompactObjectStorageDiskTransaction::commit()
{
    auto metadata_tx = disk.getMetadataStorage()->createTransaction();
    std::filesystem::path path = prefix_path;
    path /= "data.bin";
    auto object_storage = disk.getObjectStorage();
    auto object_key = object_storage->generateObjectKeyForPath(path);
    disk.createDirectories(prefix_path);
    auto write_buffer = object_storage->writeObject(DB::StoredObject(object_key.serialize(), path), DB::WriteMode::Rewrite);
    String buffer;
    buffer.resize(1024*1024);
    size_t offset = 0;

    for (const auto & item : files)
    {
        DB::DiskObjectStorageMetadata metadata(object_storage->getCommonKeyPrefix(), item.first);
        DB::ReadBufferFromFilePRead read(item.second->getAbsolutePath());
        int file_size = 0;
        while (int count = read.readBig(buffer.data(), buffer.size()))
        {
            file_size += count;
            write_buffer->write(buffer.data(), count);
        }
        metadata.addObject(object_key, offset, file_size);
        metadata_tx->writeStringToFile(item.first, metadata.serializeToString());
        //
        offset += file_size;
    }
    write_buffer->sync();
    metadata_tx->commit();
    files.clear();
}

std::unique_ptr<DB::WriteBufferFromFileBase> CompactObjectStorageDiskTransaction::writeFile(
    const std::string & path,
    size_t buf_size,
    DB::WriteMode mode,
    const DB::WriteSettings & ,
    bool)
{
    if (mode != DB::WriteMode::Rewrite)
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Operation `writeFile` with Append is not implemented");
    }
    if (prefix_path.empty())
        prefix_path = path.substr(0, path.find_last_of('/'));
    else
        if (!path.starts_with(prefix_path))
            throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Don't support write file in different dirs, path {}, prefix path: {}", path, prefix_path);
    auto tmp = std::make_shared<DB::TemporaryFileOnDisk>(tmp_data);
    files.emplace_back(path, tmp);
    return std::make_unique<DB::WriteBufferFromFile>(tmp->getAbsolutePath(), buf_size);
}
}