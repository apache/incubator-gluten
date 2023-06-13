#include <memory>
#include <Disks/IO/AsynchronousBoundedReadBuffer.h>
#include <Disks/IO/ReadBufferFromAzureBlobStorage.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Disks/ObjectStorages/AzureBlobStorage/AzureBlobStorageAuth.h>
#include <IO/CompressionMethod.h>
#include <IO/ZlibInflatingReadBuffer.h>
#include <IO/Bzip2ReadBuffer.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/S3Common.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/S3/getObjectInfo.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/HDFS/HDFSCommon.h>
#include <Storages/HDFS/ReadBufferFromHDFS.h>
#include <Storages/StorageS3Settings.h>
#include <Storages/SubstraitSource/ReadBufferBuilder.h>
#include <Storages/SubstraitSource/SubstraitFileSource.h>
#include <aws/core/client/DefaultRetryStrategy.h>

#include <sys/stat.h>
#include <Poco/URI.h>
#include "IO/ReadSettings.h"

#include <Common/Throttler.h>
#include <Common/safe_cast.h>
#include <Common/logger_useful.h>
#include <Poco/Logger.h>
#include <hdfs/hdfs.h>

#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Cache/FileCacheFactory.h>
#include <Interpreters/Cache/FileCacheSettings.h>

#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_OPEN_FILE;
    extern const int UNKNOWN_FILE_SIZE;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int CANNOT_CLOSE_FILE;
}
}

namespace local_engine
{
class LocalFileReadBufferBuilder : public ReadBufferBuilder
{
public:
    explicit LocalFileReadBufferBuilder(DB::ContextPtr context_) : ReadBufferBuilder(context_) { }
    ~LocalFileReadBufferBuilder() override = default;

    std::unique_ptr<DB::ReadBuffer> build(const substrait::ReadRel::LocalFiles::FileOrFiles & file_info, const bool &, const DB::CompressionMethod &) override
    {
        Poco::URI file_uri(file_info.uri_file());
        std::unique_ptr<DB::ReadBuffer> read_buffer;
        const String & file_path = file_uri.getPath();
        struct stat file_stat;
        if (stat(file_path.c_str(), &file_stat))
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "file stat failed for {}", file_path);

        if (S_ISREG(file_stat.st_mode))
            read_buffer = std::make_unique<DB::ReadBufferFromFilePRead>(file_path);
        else
            read_buffer = std::make_unique<DB::ReadBufferFromFilePRead>(file_path);
        return read_buffer;
    }
};

#if USE_HDFS
class HDFSFileReadBufferBuilder : public ReadBufferBuilder
{
public:
    explicit HDFSFileReadBufferBuilder(DB::ContextPtr context_) : ReadBufferBuilder(context_) { }
    ~HDFSFileReadBufferBuilder() override = default;

    std::unique_ptr<DB::ReadBuffer> build(
        const substrait::ReadRel::LocalFiles::FileOrFiles & file_info,
        const bool & set_read_util_position,
        const DB::CompressionMethod & txt_compression_method) override
    {
        Poco::URI file_uri(file_info.uri_file());
        std::unique_ptr<DB::ReadBuffer> read_buffer;
        std::string uri_path = "hdfs://" + file_uri.getHost();
        if (file_uri.getPort())
            uri_path += ":" + std::to_string(file_uri.getPort());
        DB::ReadSettings read_settings;
        if (set_read_util_position) 
        {
            std::pair<size_t, size_t> start_end_pos = adjustFileReadStartAndEndPos(file_info.start(), file_info.start() + file_info.length(),
                    uri_path, file_uri.getPath());
            LOG_DEBUG(&Poco::Logger::get("ReadBufferBuilder"), "File read start and end position adjusted from {},{} to {},{}",
                    file_info.start(), file_info.start() + file_info.length(), start_end_pos.first, start_end_pos.second);
            read_buffer = std::make_unique<DB::ReadBufferFromHDFS>(
                uri_path, file_uri.getPath(), context->getGlobalContext()->getConfigRef(),
                read_settings, start_end_pos.second);
            if (auto * seekable_in = dynamic_cast<DB::SeekableReadBuffer *>(read_buffer.get()))
            {
                seekable_in->seek(start_end_pos.first, SEEK_SET);
            }
        }
        else
        {
            read_buffer = std::make_unique<DB::ReadBufferFromHDFS>(
                uri_path, file_uri.getPath(), context->getGlobalContext()->getConfigRef(),
                read_settings);
        }
        if (txt_compression_method == DB::CompressionMethod::Zlib)
        {
            read_buffer = std::make_unique<DB::ZlibInflatingReadBuffer>(std::move(read_buffer), txt_compression_method);
        }
        else if (txt_compression_method == DB::CompressionMethod::Bzip2)
        {
            read_buffer = std::make_unique<DB::Bzip2ReadBuffer>(std::move(read_buffer));
        }
        return read_buffer;
    }

    std::pair<size_t, size_t> adjustFileReadStartAndEndPos(
        size_t read_start_pos,
        size_t read_end_pos,
        std::string uri_path,
        std::string file_path)
    {
        std::pair<size_t, size_t> result;
        std::string row_delimiter = "\n";
        size_t row_delimiter_size = row_delimiter.size();
        std::string hdfs_file_path = uri_path + file_path;
        auto builder = DB::createHDFSBuilder(hdfs_file_path, context->getGlobalContext()->getConfigRef());
        auto fs = DB::createHDFSFS(builder.get());
        hdfsFile fin = hdfsOpenFile(fs.get(), file_path.c_str(), O_RDONLY, 0, 0, 0);
        if (!fin)
        {
            throw DB::Exception(DB::ErrorCodes::CANNOT_OPEN_FILE, "Cannot open hdfs file:{}, error: {}", hdfs_file_path, std::string(hdfsGetLastError()));
        }
        auto hdfs_file_info = hdfsGetPathInfo(fs.get(), file_path.c_str());
        if (!hdfs_file_info)
        {
            hdfsCloseFile(fs.get(), fin);
            throw DB::Exception(DB::ErrorCodes::UNKNOWN_FILE_SIZE, "Cannot find out file size for :{}, error: {}", hdfs_file_path, std::string(hdfsGetLastError()));
        }
        size_t hdfs_file_size = hdfs_file_info->mSize;
        auto getFirstRowDelimiterPos = [&](hdfsFS fs, hdfsFile fin, size_t start_pos, size_t hdfs_file_size) -> size_t
        {
            if (start_pos == 0 || start_pos == hdfs_file_size)
            {
                return start_pos;
            }
            size_t pos = start_pos;
            int seek_status = hdfsSeek(fs, fin, pos);
            if (seek_status != 0)
            {
                hdfsCloseFile(fs, fin);
                throw DB::Exception(DB::ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Fail to seek HDFS file: {}, error: {}", file_path, std::string(hdfsGetLastError()));
            }
            char s[row_delimiter_size];
            bool read_flag = true;
            while (read_flag)
            {
                size_t read_size = hdfsRead(fs, fin, s, row_delimiter_size);
                size_t i = 0;
                for (; i < read_size && i < row_delimiter_size; ++i)
                {
                    if (s[i] != *(row_delimiter.data() + i))
                    {
                        break;
                    }
                }
                if (i == row_delimiter_size)
                {
                    char r[1];
                    // The end of row maybe '\n', '\r\n', or '\n\r'
                    if (hdfsRead(fs, fin, r, 1) != 0 && r[0] == '\r')
                    {
                        return pos + 1 + row_delimiter_size;
                    }
                    else
                    {
                        return pos + row_delimiter_size;
                    }
                }
                else
                {
                    pos += 1;
                    hdfsSeek(fs, fin, pos);
                }
            }
        };
        result.first = getFirstRowDelimiterPos(fs.get(), fin, read_start_pos, hdfs_file_size);
        result.second = getFirstRowDelimiterPos(fs.get(), fin, read_end_pos, hdfs_file_size);
        int close_status = hdfsCloseFile(fs.get(), fin);
        if (close_status != 0)
        {
            throw DB::Exception(DB::ErrorCodes::CANNOT_CLOSE_FILE, "Fail to close HDFS file: {}, error: {}", file_path, std::string(hdfsGetLastError()));
        }
        return result;
    }
};
#endif

#if USE_AWS_S3
class S3FileReadBufferBuilder : public ReadBufferBuilder
{
public:
    explicit S3FileReadBufferBuilder(DB::ContextPtr context_) : ReadBufferBuilder(context_)
    {
        DB::FileCacheSettings file_cache_settings;
        file_cache_settings.max_size = static_cast<size_t>(context->getConfigRef().getUInt64("s3.local_cache.max_size", 100L << 30));
        auto cache_base_path = context->getConfigRef().getString("s3.local_cache.cache_path", "/tmp/gluten/local_cache");
        if (!fs::exists(cache_base_path))
            fs::create_directories(cache_base_path);
        file_cache_settings.base_path = cache_base_path;
        new_settings = DB::ReadSettings();
        new_settings.enable_filesystem_cache = context->getConfigRef().getBool("s3.local_cache.enabled", false);
        if (new_settings.enable_filesystem_cache)
        {
            auto cache = DB::FileCacheFactory::instance().getOrCreate("s3_local_cache", file_cache_settings);
            cache->initialize();

            new_settings.remote_fs_cache = cache;
        }
    }

    ~S3FileReadBufferBuilder() override = default;

    std::unique_ptr<DB::ReadBuffer> build(const substrait::ReadRel::LocalFiles::FileOrFiles & file_info, const bool &, const DB::CompressionMethod &) override
    {
        Poco::URI file_uri(file_info.uri_file());
        const auto client = getClient();
        // file uri looks like: s3a://my-dev-bucket/tpch100/part/0001.parquet
        std::string bucket = file_uri.getHost();
        std::string key = file_uri.getPath().substr(1);
        size_t object_size = DB::S3::getObjectSize(*client, bucket, key, "");

        auto read_buffer_creator
            = [bucket, this](const std::string & path, size_t read_until_position) -> std::unique_ptr<DB::ReadBufferFromFileBase>
        {
            return std::make_unique<DB::ReadBufferFromS3>(
                shared_client,
                bucket,
                path,
                "",
                DB::S3Settings::RequestSettings(),
                new_settings,
                /* use_external_buffer */ true,
                /* offset */ 0,
                read_until_position,
                /* restricted_seek */ true);
        };

        DB::StoredObjects stored_objects{DB::StoredObject{key, object_size}};
        auto s3_impl = std::make_unique<DB::ReadBufferFromRemoteFSGather>(
            std::move(read_buffer_creator), stored_objects, new_settings, /* cache_log */ nullptr, /* use_external_buffer */ false);

        auto & pool_reader = context->getThreadPoolReader(DB::FilesystemReaderType::ASYNCHRONOUS_REMOTE_FS_READER);
        auto async_reader
            = std::make_unique<DB::AsynchronousBoundedReadBuffer>(std::move(s3_impl), pool_reader, new_settings, nullptr, nullptr);

        async_reader->setReadUntilEnd();
        if (new_settings.remote_fs_prefetch)
            async_reader->prefetch(Priority{});

        return async_reader;
    }

private:
    std::shared_ptr<DB::S3::Client> shared_client;
    DB::ReadSettings new_settings;


    std::shared_ptr<DB::S3::Client> getClient()
    {
        if (shared_client)
            return shared_client;

        const auto & config = context->getConfigRef();
        String config_prefix = "s3";
        auto endpoint = config.getString(config_prefix + ".endpoint", "https://s3.us-west-2.amazonaws.com");
        String region_name;
        const char * amazon_suffix = ".amazonaws.com";
        const char * amazon_prefix = "https://s3.";
        auto pos = endpoint.find(amazon_suffix);
        if (pos != std::string::npos)
        {
            assert(endpoint.starts_with(amazon_prefix));
            region_name = endpoint.substr(strlen(amazon_prefix), pos - strlen(amazon_prefix));
            assert(region_name.find('.') == std::string::npos);
        }
        // for AWS CN, the endpoint is like: https://s3.cn-north-1.amazonaws.com.cn, still works

        DB::S3::PocoHTTPClientConfiguration client_configuration = DB::S3::ClientFactory::instance().createClientConfiguration(
            region_name,
            context->getRemoteHostFilter(),
            context->getGlobalContext()->getSettingsRef().s3_max_redirects,
            false,
            false,
            nullptr,
            nullptr);

        client_configuration.connectTimeoutMs = config.getUInt(config_prefix + ".connect_timeout_ms", 10000);
        client_configuration.requestTimeoutMs = config.getUInt(config_prefix + ".request_timeout_ms", 5000);
        client_configuration.maxConnections = config.getUInt(config_prefix + ".max_connections", 100);
        client_configuration.endpointOverride = endpoint;

        client_configuration.retryStrategy
            = std::make_shared<Aws::Client::DefaultRetryStrategy>(config.getUInt(config_prefix + ".retry_attempts", 10));

        shared_client = DB::S3::ClientFactory::instance().create(
            client_configuration,
            false,
            config.getString(config_prefix + ".access_key_id", ""),
            config.getString(config_prefix + ".secret_access_key", ""),
            config.getString(config_prefix + ".server_side_encryption_customer_key_base64", ""),
            {},
            {},
            {.use_environment_credentials
             = config.getBool(config_prefix + ".use_environment_credentials", config.getBool("s3.use_environment_credentials", false)),
             .use_insecure_imds_request
             = config.getBool(config_prefix + ".use_insecure_imds_request", config.getBool("s3.use_insecure_imds_request", false))});
        return shared_client;
    }
};
#endif

#if USE_AZURE_BLOB_STORAGE
class AzureBlobReadBuffer : public ReadBufferBuilder
{
public:
    explicit AzureBlobReadBuffer(DB::ContextPtr context_) : ReadBufferBuilder(context_) { }
    ~AzureBlobReadBuffer() override = default;

    std::unique_ptr<DB::ReadBuffer> build(const substrait::ReadRel::LocalFiles::FileOrFiles & file_info, const bool &, const DB::CompressionMethod &)
    {
        Poco::URI file_uri(file_info.uri_file());
        std::unique_ptr<DB::ReadBuffer> read_buffer;
        read_buffer = std::make_unique<DB::ReadBufferFromAzureBlobStorage>(getClient(), file_uri.getPath(), DB::ReadSettings(), 5, 5);
        return read_buffer;
    }

private:
    std::shared_ptr<Azure::Storage::Blobs::BlobContainerClient> shared_client;

    std::shared_ptr<Azure::Storage::Blobs::BlobContainerClient> getClient()
    {
        if (shared_client)
            return shared_client;
        shared_client = DB::getAzureBlobContainerClient(context->getConfigRef(), "blob");
        return shared_client;
    }
};
#endif

void registerReadBufferBuilders()
{
    auto & factory = ReadBufferBuilderFactory::instance();
    factory.registerBuilder("file", [](DB::ContextPtr context_) { return std::make_shared<LocalFileReadBufferBuilder>(context_); });

#if USE_HDFS
    factory.registerBuilder("hdfs", [](DB::ContextPtr context_) { return std::make_shared<HDFSFileReadBufferBuilder>(context_); });
#endif

#if USE_AWS_S3
    factory.registerBuilder("s3", [](DB::ContextPtr context_) { return std::make_shared<S3FileReadBufferBuilder>(context_); });
    factory.registerBuilder("s3a", [](DB::ContextPtr context_) { return std::make_shared<S3FileReadBufferBuilder>(context_); });
#endif

#if USE_AZURE_BLOB_STORAGE
    factory.registerBuilder("wasb", [](DB::ContextPtr context_) { return std::make_shared<AzureBlobReadBuffer>(context_); });
    factory.registerBuilder("wasbs", [](DB::ContextPtr context_) { return std::make_shared<AzureBlobReadBuffer>(context_); });
#endif
}

ReadBufferBuilderFactory & ReadBufferBuilderFactory::instance()
{
    static ReadBufferBuilderFactory instance;
    return instance;
}

ReadBufferBuilderPtr ReadBufferBuilderFactory::createBuilder(const String & schema, DB::ContextPtr context)
{
    auto it = builders.find(schema);
    if (it == builders.end())
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Not found read buffer builder for {}", schema);
    return it->second(context);
}

void ReadBufferBuilderFactory::registerBuilder(const String & schema, NewBuilder newer)
{
    auto it = builders.find(schema);
    if (it != builders.end())
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "readbuffer builder for {} has been registered", schema);
    builders[schema] = newer;
}

}
