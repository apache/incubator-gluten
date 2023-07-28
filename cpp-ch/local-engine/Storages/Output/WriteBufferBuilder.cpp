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
#include <Interpreters/Context_fwd.h>
#include <Storages/HDFS/HDFSCommon.h>
#include <Storages/HDFS/WriteBufferFromHDFS.h>
#include <Storages/Output/WriteBufferBuilder.h>
#include <hdfs/hdfs.h>
#include <Poco/URI.h>
#include "IO/WriteSettings.h"

#include <Poco/Logger.h>

#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Cache/FileCacheSettings.h>

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
    explicit LocalFileWriteBufferBuilder(DB::ContextPtr context_) : WriteBufferBuilder(context_) { }
    ~LocalFileWriteBufferBuilder() override = default;

    std::unique_ptr<DB::WriteBuffer> build(const std::string & file_uri_) override
    {
        Poco::URI file_uri(file_uri_);
        const String & file_path = file_uri.getPath();

        //mkdir
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
    explicit HDFSFileWriteBufferBuilder(DB::ContextPtr context_) : WriteBufferBuilder(context_) { }
    ~HDFSFileWriteBufferBuilder() override = default;

    std::unique_ptr<DB::WriteBuffer> build(const std::string & file_uri_) override
    {
        Poco::URI file_uri(file_uri_);
        std::unique_ptr<DB::WriteBuffer> write_buffer;

        auto builder = DB::createHDFSBuilder(file_uri_, context->getConfigRef());
        auto fs = DB::createHDFSFS(builder.get());
        auto first = file_uri_.find('/', file_uri_.find("//") + 2);
        auto last = file_uri_.find_last_of('/');
        auto dir = file_uri_.substr(first, last - first);
        int err = hdfsCreateDirectory(fs.get(), dir.c_str());
        if (err)
        {
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Cannot create dir for {}", dir);
        }

        DB::WriteSettings write_settings;
        write_buffer = std::make_unique<DB::WriteBufferFromHDFS>(file_uri_, context->getConfigRef(), 0, write_settings);
        return write_buffer;
    }
};
#endif


void registerWriteBufferBuilders()
{
    auto & factory = WriteBufferBuilderFactory::instance();
    //TODO: support azure and S3
    factory.registerBuilder("file", [](DB::ContextPtr context_) { return std::make_shared<LocalFileWriteBufferBuilder>(context_); });
    factory.registerBuilder("hdfs", [](DB::ContextPtr context_) { return std::make_shared<HDFSFileWriteBufferBuilder>(context_); });
}

WriteBufferBuilderFactory & WriteBufferBuilderFactory::instance()
{
    static WriteBufferBuilderFactory instance;
    return instance;
}

WriteBufferBuilderPtr WriteBufferBuilderFactory::createBuilder(const String & schema, DB::ContextPtr context)
{
    auto it = builders.find(schema);
    if (it == builders.end())
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Not found write buffer builder for {}", schema);
    return it->second(context);
}

void WriteBufferBuilderFactory::registerBuilder(const String & schema, NewBuilder newer)
{
    auto it = builders.find(schema);
    if (it != builders.end())
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "write buffer builder for {} has been registered", schema);
    builders[schema] = newer;
}

}
