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

#include "ProtobufUtils.h"

#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/util/json_util.h>
#include <google/protobuf/util/type_resolver.h>
#include <google/protobuf/util/type_resolver_util.h>
#include <fstream>
#include <iostream>
#include <string>

// Common for both projector and filters.

bool ParseProtobuf(const uint8_t* buf, int bufLen, google::protobuf::Message* msg) {
  google::protobuf::io::CodedInputStream coded_stream{buf, bufLen};
  // The default recursion limit is 100 which is too smaller for a deep
  // Substrait plan.
  coded_stream.SetRecursionLimit(100000);
  return msg->ParseFromCodedStream(&coded_stream);
}

inline google::protobuf::util::TypeResolver* GetGeneratedTypeResolver() {
  static std::unique_ptr<google::protobuf::util::TypeResolver> type_resolver;
  static std::once_flag type_resolver_init;
  std::call_once(type_resolver_init, []() {
    type_resolver.reset(google::protobuf::util::NewTypeResolverForDescriptorPool(
        /*url_prefix=*/"", google::protobuf::DescriptorPool::generated_pool()));
  });
  return type_resolver.get();
}

arrow::Result<std::shared_ptr<arrow::Buffer>> SubstraitFromJSON(
    arrow::util::string_view type_name,
    arrow::util::string_view json) {
  std::string type_url = "/substrait." + type_name.to_string();

  google::protobuf::io::ArrayInputStream json_stream{json.data(), static_cast<int>(json.size())};

  std::string out;
  google::protobuf::io::StringOutputStream out_stream{&out};

  auto status =
      google::protobuf::util::JsonToBinaryStream(GetGeneratedTypeResolver(), type_url, &json_stream, &out_stream);

  if (!status.ok()) {
    return arrow::Status::Invalid("JsonToBinaryStream returned ", status);
  }
  return arrow::Buffer::FromString(std::move(out));
}

arrow::Result<std::string> SubstraitToJSON(arrow::util::string_view type_name, const arrow::Buffer& buf) {
  std::string type_url = "/substrait." + type_name.to_string();

  google::protobuf::io::ArrayInputStream buf_stream{buf.data(), static_cast<int>(buf.size())};

  std::string out;
  google::protobuf::io::StringOutputStream out_stream{&out};

  auto status =
      google::protobuf::util::BinaryToJsonStream(GetGeneratedTypeResolver(), type_url, &buf_stream, &out_stream);
  if (!status.ok()) {
    return arrow::Status::Invalid("BinaryToJsonStream returned ", status);
  }
  return out;
}

void MessageToJSONFile(const google::protobuf::Message& message, const std::string& file_path) {
  google::protobuf::util::JsonPrintOptions options;
  options.add_whitespace = true;
  options.always_print_primitive_fields = true;
  options.preserve_proto_field_names = true;
  std::string json_string;
  google::protobuf::util::MessageToJsonString(message, &json_string, options);
  std::cin >> json_string;
  std::ofstream out(file_path);
  out << json_string;
  out.close();
}
