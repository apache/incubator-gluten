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

#include <filesystem>
#include <string>
#include <Poco/TemporaryFile.h>
#include <Common/Exception.h>

namespace fs = std::filesystem;

namespace local_engine::test
{
class TempFilePath
{
    static constexpr auto kTempDir = "/tmp/gluten";

    const std::string path_;

public:
    static std::shared_ptr<TempFilePath> tmp(const std::string & extension = "")
    {
        fs::path tempPath(Poco::TemporaryFile::tempName(kTempDir));
        tempPath.replace_extension(extension);
        return std::make_shared<TempFilePath>(tempPath.string());
    }

    explicit TempFilePath() : path_{Poco::TemporaryFile::tempName(kTempDir)} { }
    explicit TempFilePath(const std::string & path) : path_{path} { }

    ~TempFilePath()
    {
        try
        {
            if (const fs::path tmp{path_}; exists(tmp))
                remove(tmp);
        }
        catch (...)
        {
            DB::tryLogCurrentException(__PRETTY_FUNCTION__, fmt::format("Error while removing temporary directory {}:", path_));
        }
    }

    const std::string & string() const { return path_; }


    TempFilePath(const TempFilePath &) = delete;
    TempFilePath & operator=(const TempFilePath &) = delete;
};

}
