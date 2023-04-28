#include <memory>
#include <Storages/SubstraitSource/ReadBufferBuilder.h>
#include "IO/ReadSettings.h"
#include <Disks/IO/ReadBufferFromAzureBlobStorage.h>
#include <Storages/SubstraitSource/SubstraitFileSource.h>
#include <Storages/HDFS/ReadBufferFromHDFS.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/S3Common.h>
#include <Interpreters/Context_fwd.h>
#include <sys/stat.h>
#include <Poco/URI.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/s3/S3Client.h>
#include <Storages/StorageS3Settings.h>
#include <Disks/ObjectStorages/AzureBlobStorage/AzureBlobStorageAuth.h>

#include <Poco/Logger.h>
#include <Common/logger_useful.h>

#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Cache/FileCacheFactory.h>
#include <Interpreters/Cache/FileCacheSettings.h>

#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <IO/S3/getObjectInfo.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
}

namespace local_engine
{

class LocalFileReadBufferBuilder : public ReadBufferBuilder
{
public:
    explicit LocalFileReadBufferBuilder(DB::ContextPtr context_) : ReadBufferBuilder(context_) { }
    ~LocalFileReadBufferBuilder() override = default;

    std::unique_ptr<DB::ReadBuffer> build(const substrait::ReadRel::LocalFiles::FileOrFiles & file_info) override
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

    std::unique_ptr<DB::ReadBuffer> build(const substrait::ReadRel::LocalFiles::FileOrFiles & file_info) override
    {
        Poco::URI file_uri(file_info.uri_file());
        std::unique_ptr<DB::ReadBuffer> read_buffer;

        std::string uri_path = "hdfs://" + file_uri.getHost();
        if (file_uri.getPort())
            uri_path += ":" + std::to_string(file_uri.getPort());
        DB::ReadSettings read_settings;
        read_buffer = std::make_unique<DB::ReadBufferFromHDFS>(
            uri_path, file_uri.getPath(), context->getGlobalContext()->getConfigRef(),
            read_settings);
        return read_buffer;
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

        new_settings = DB::ReadSettings();
        new_settings.enable_filesystem_cache = context->getConfigRef().getBool("s3.local_cache.enabled", false);
        if (new_settings.enable_filesystem_cache)
        {
            auto cache = DB::FileCacheFactory::instance().getOrCreate(cache_base_path, file_cache_settings, "s3_local_cache");
            cache->initialize();

            new_settings.remote_fs_cache = cache;
        }
    }

    ~S3FileReadBufferBuilder() override = default;

    std::unique_ptr<DB::ReadBuffer> build(const substrait::ReadRel::LocalFiles::FileOrFiles & file_info) override
    {
        Poco::URI file_uri(file_info.uri_file());
        const auto client = getClient();
        // file uri looks like: s3a://my-dev-bucket/tpch100/part/0001.parquet
        std::string bucket = file_uri.getHost();
        std::string key = file_uri.getPath().substr(1);
        size_t object_size = DB::S3::getObjectSize(*client, bucket, key, "");

        auto read_buffer_creator =
            [bucket, this](const std::string & path, size_t read_until_position) -> std::shared_ptr<DB::ReadBufferFromFileBase>
        {
            return std::make_shared<DB::ReadBufferFromS3>(
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

        auto s3_impl = std::make_unique<DB::ReadBufferFromRemoteFSGather>(
            std::move(read_buffer_creator), DB::StoredObjects{DB::StoredObject{key, object_size}}, new_settings);

        auto & pool_reader = context->getThreadPoolReader(DB::Context::FilesystemReaderType::ASYNCHRONOUS_REMOTE_FS_READER);
        auto async_reader = std::make_unique<DB::AsynchronousReadIndirectBufferFromRemoteFS>(pool_reader, new_settings, std::move(s3_impl));

        async_reader->setReadUntilEnd();
        if (new_settings.remote_fs_prefetch)
            async_reader->prefetch(0);

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
        if (endpoint.find(amazon_suffix) != std::string::npos)
        {
            assert(endpoint.starts_with(amazon_prefix));
            region_name = endpoint.substr(strlen(amazon_prefix), endpoint.size() - strlen(amazon_prefix) - strlen(amazon_suffix));
            assert(region_name.find('.') == std::string::npos);
        }
        // for AWS CN, the endpoint is like: https://s3.cn-north-1.amazonaws.com.cn, should still work with above code (NOT TESTED)

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
            {
                .use_environment_credentials=config.getBool(config_prefix + ".use_environment_credentials", config.getBool("s3.use_environment_credentials", false)),
                .use_insecure_imds_request=config.getBool(config_prefix + ".use_insecure_imds_request", config.getBool("s3.use_insecure_imds_request", false))
            });
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

    std::unique_ptr<DB::ReadBuffer> build(const substrait::ReadRel::LocalFiles::FileOrFiles & file_info)
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
