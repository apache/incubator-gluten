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
#include <functional>
#include <memory>
#include <IO/ReadBuffer.h>
#include <Interpreters/Context_fwd.h>
#include <boost/core/noncopyable.hpp>
#include <substrait/plan.pb.h>

namespace local_engine
{
class ReadBufferBuilder
{
public:
    explicit ReadBufferBuilder(DB::ContextPtr context_) : context(context_) { }
    virtual ~ReadBufferBuilder() = default;

    /// build a new read buffer
    virtual std::unique_ptr<DB::ReadBuffer>
    build(const substrait::ReadRel::LocalFiles::FileOrFiles & file_info, bool set_read_util_position = false) = 0;

    /// build a new read buffer, consider compression method
    std::unique_ptr<DB::ReadBuffer>
    buildWithCompressionWrapper(const substrait::ReadRel::LocalFiles::FileOrFiles & file_info, bool set_read_util_position = false)
    {
        auto in = build(file_info, set_read_util_position);

        /// Wrap the read buffer with compression method if exists
        Poco::URI file_uri(file_info.uri_file());
        DB::CompressionMethod compression = DB::chooseCompressionMethod(file_uri.getPath(), "auto");
        return compression != DB::CompressionMethod::None ? DB::wrapReadBufferWithCompressionMethod(std::move(in), compression)
                                                          : std::move(in);
    }

protected:
    DB::ContextPtr context;
};

using ReadBufferBuilderPtr = std::shared_ptr<ReadBufferBuilder>;

class ReadBufferBuilderFactory : public boost::noncopyable
{
public:
    static ReadBufferBuilderFactory & instance();

    using NewBuilder = std::function<ReadBufferBuilderPtr(DB::ContextPtr)>;
    void registerBuilder(const String & schema, NewBuilder newer);
    ReadBufferBuilderPtr createBuilder(const String & schema, DB::ContextPtr context);

    using Cleaner = std::function<void()>; // used to clean cache and so on for each kind of builder
    void registerCleaner(Cleaner);
    void clean();


private:
    std::map<String, NewBuilder> builders;
    std::vector<Cleaner> cleaners;
};

void registerReadBufferBuilders();
}
