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

#include "config.h"

#if USE_BZIP2
#include <IO/BoundedReadBuffer.h>
#include <IO/ReadSettings.h>
#include <IO/SplittableBzip2ReadBuffer.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/copyData.h>
#include <Storages/ObjectStorage/HDFS/ReadBufferFromHDFS.h>
#include <Poco/Util/MapConfiguration.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/LoggerExtend.h>

using namespace DB;

int main()
{
    local_engine::LoggerExtend::initConsoleLogger("debug");

    setenv("LIBHDFS3_CONF", "/path/to/hdfs/config", true); /// NOLINT
    String hdfs_uri = "hdfs://cluster";
    String hdfs_file_path = "/path/to/bzip2/file";
    ConfigurationPtr config = Poco::AutoPtr(new Poco::Util::MapConfiguration());
    ReadSettings read_settings;
    std::unique_ptr<SeekableReadBuffer> in
        = std::make_unique<ReadBufferFromHDFS>(hdfs_uri, hdfs_file_path, *config, read_settings, 0, false);

    std::unique_ptr<SeekableReadBuffer> bounded_in = std::make_unique<BoundedReadBuffer>(std::move(in));
    size_t start = 0;
    size_t end = 268660564;
    bounded_in->seek(start, SEEK_SET);
    bounded_in->setReadUntilPosition(end);

    std::unique_ptr<ReadBuffer> decompressed = std::make_unique<SplittableBzip2ReadBuffer>(std::move(bounded_in), false, true);

    String download_path = "./download_" + std::to_string(start) + "_" + std::to_string(end) + ".txt";
    WriteBufferFromFile write_buffer(download_path);
    copyData(*decompressed, write_buffer);
    return 0;
}
#endif
