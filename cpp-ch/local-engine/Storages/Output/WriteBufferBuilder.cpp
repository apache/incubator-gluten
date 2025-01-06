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
#include <memory>
#include <IO/WriteSettings.h>
#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/HDFS/HDFSCommon.h>
#include <Storages/ObjectStorage/HDFS/WriteBufferFromHDFS.h>
#include <Storages/Output/WriteBufferBuilder.h>
#if USE_HDFS
#include <hdfs/hdfs.h>
#endif
#include <Poco/URI.h>
#include <Common/CHUtil.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}
}

namespace local_engine
{

class LocalFileWriteBufferBuilder : public WriteBufferBuilder
{
public:
    explicit LocalFileWriteBufferBuilder(const DB::ContextPtr & context_) : WriteBufferBuilder(context_) { }
    ~LocalFileWriteBufferBuilder() override = default;

    std::unique_ptr<DB::WriteBuffer> build(const std::string & file_uri_) override
    {
        Poco::URI file_uri(file_uri_);
        const String & file_path = file_uri.getPath();

        // mkdir
        std::filesystem::path p(file_path);
        if (!std::filesystem::exists(p.parent_path()))
            std::filesystem::create_directories(p.parent_path());

        return std::make_unique<DB::WriteBufferFromFile>(file_path);
    }
};


#if USE_HDFS
class HDFSFileWriteBufferBuilder : public WriteBufferBuilder
{
public:
    explicit HDFSFileWriteBufferBuilder(const DB::ContextPtr & context_) : WriteBufferBuilder(context_) { }
    ~HDFSFileWriteBufferBuilder() override = default;

    std::unique_ptr<DB::WriteBuffer> build(const std::string & file_uri_) override
    {
        Poco::URI uri(file_uri_);

        /// Add spark user for file_uri to avoid permission issue during native writing
        std::string new_file_uri = file_uri_;
        if (uri.getUserInfo().empty() && BackendInitializerUtil::spark_user.has_value())
        {
            uri.setUserInfo(*BackendInitializerUtil::spark_user);
            new_file_uri = uri.toString();
        }

        auto builder = DB::createHDFSBuilder(new_file_uri, context->getConfigRef());
        auto fs = DB::createHDFSFS(builder.get());

        auto begin_of_path = new_file_uri.find('/', new_file_uri.find("//") + 2);
        auto url_without_path = new_file_uri.substr(0, begin_of_path);

        // use uri.getPath() instead of new_file_uri.substr(begin_of_path) to avoid space character uri-encoded
        std::filesystem::path file_path(uri.getPath());
        auto dir = file_path.parent_path().string();

        if (hdfsCreateDirectory(fs.get(), dir.c_str()))
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Cannot create dir for {} because {}", dir, std::string(hdfsGetLastError()));

        DB::WriteSettings write_settings;
        return std::make_unique<DB::WriteBufferFromHDFS>(url_without_path, file_path.string(), context->getConfigRef(), 0, write_settings);
    }
};
#endif


void registerWriteBufferBuilders()
{
    auto & factory = WriteBufferBuilderFactory::instance();
    //TODO: support azure and S3
    factory.registerBuilder("file", [](DB::ContextPtr context_) { return std::make_shared<LocalFileWriteBufferBuilder>(context_); });
#if USE_HDFS
    factory.registerBuilder("hdfs", [](DB::ContextPtr context_) { return std::make_shared<HDFSFileWriteBufferBuilder>(context_); });
#endif
}

WriteBufferBuilderFactory & WriteBufferBuilderFactory::instance()
{
    static WriteBufferBuilderFactory instance;
    return instance;
}

WriteBufferBuilderPtr WriteBufferBuilderFactory::createBuilder(const String & schema, const DB::ContextPtr & context)
{
    auto it = builders.find(schema);
    if (it == builders.end())
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Not found write buffer builder for {}", schema);
    return it->second(context);
}

void WriteBufferBuilderFactory::registerBuilder(const String & schema, const NewBuilder & newer)
{
    auto it = builders.find(schema);
    if (it != builders.end())
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "write buffer builder for {} has been registered", schema);
    builders[schema] = newer;
}

}
