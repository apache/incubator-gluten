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

#include <string>
#include <google/protobuf/util/json_util.h>
#include <Common/Exception.h>

namespace DB::ErrorCodes
{
extern const int CANNOT_PARSE_PROTOBUF_SCHEMA;
}

namespace local_engine
{

template <typename Message>
Message JsonStringToMessage(std::string_view json)
{
    Message message;
    auto status = google::protobuf::util::JsonStringToMessage(json, &message);
    if (!status.ok())
    {
        std::string errmsg(status.message());
        throw DB::Exception(DB::ErrorCodes::CANNOT_PARSE_PROTOBUF_SCHEMA, "Parse failed due to {}", errmsg);
    }
    return message;
}

template <typename Message>
std::string JsonStringToBinary(const std::string_view json)
{
    Message message = JsonStringToMessage<Message>(json);
    std::string binary;
    message.SerializeToString(&binary);
    return binary;
}

template <typename Message>
Message BinaryToMessage(const std::string_view binary)
{
    Message message;
    /// https://stackoverflow.com/questions/52028583/getting-error-parsing-protobuf-data
    /// Parsing may fail when the number of recursive layers is large.
    /// Here, set a limit large enough to avoid this problem.
    /// Once this problem occurs, it is difficult to troubleshoot, because the pb of c++ will not provide any valid information
    google::protobuf::io::CodedInputStream coded_in(reinterpret_cast<const uint8_t *>(binary.data()), static_cast<int>(binary.size()));
    coded_in.SetRecursionLimit(100000);

    if (!message.ParseFromCodedStream(&coded_in))
        throw DB::Exception(DB::ErrorCodes::CANNOT_PARSE_PROTOBUF_SCHEMA, "Parse failed");
    return message;
}

} // namespace local_engine
