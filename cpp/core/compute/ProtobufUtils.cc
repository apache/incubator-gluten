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

#include <google/protobuf/util/json_util.h>
#include <google/protobuf/util/type_resolver_util.h>

#include "utils/exception.h"

namespace gluten {

// Common for both projector and filters.

bool parseProtobuf(const uint8_t* buf, int bufLen, google::protobuf::Message* msg) {
  google::protobuf::io::CodedInputStream codedStream{buf, bufLen};
  // The default recursion limit is 100 which is too smaller for a deep
  // Substrait plan.
  codedStream.SetRecursionLimit(100000);
  return msg->ParseFromCodedStream(&codedStream);
}

inline google::protobuf::util::TypeResolver* getGeneratedTypeResolver() {
  static std::unique_ptr<google::protobuf::util::TypeResolver> typeResolver;
  static std::once_flag typeResolverInit;
  std::call_once(typeResolverInit, []() {
    typeResolver.reset(google::protobuf::util::NewTypeResolverForDescriptorPool(
        /*url_prefix=*/"", google::protobuf::DescriptorPool::generated_pool()));
  });
  return typeResolver.get();
}

std::string substraitFromJsonToPb(std::string_view typeName, std::string_view json) {
  std::string typeUrl = "/substrait." + std::string(typeName);

  google::protobuf::io::ArrayInputStream jsonStream{json.data(), static_cast<int>(json.size())};

  std::string out;
  google::protobuf::io::StringOutputStream outStream{&out};

  auto status =
      google::protobuf::util::JsonToBinaryStream(getGeneratedTypeResolver(), typeUrl, &jsonStream, &outStream);

  if (!status.ok()) {
    throw GlutenException("JsonToBinaryStream returned " + status.ToString());
  }
  return out;
}

std::string substraitFromPbToJson(std::string_view typeName, const uint8_t* data, int32_t size) {
  std::string typeUrl = "/substrait." + std::string(typeName);

  google::protobuf::io::ArrayInputStream bufStream{data, size};

  std::string out;
  google::protobuf::io::StringOutputStream outStream{&out};

  auto status = google::protobuf::util::BinaryToJsonStream(getGeneratedTypeResolver(), typeUrl, &bufStream, &outStream);
  if (!status.ok()) {
    throw GlutenException("BinaryToJsonStream returned " + status.ToString());
  }
  return out;
}

/*
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
*/

} // namespace gluten
