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

#include <boost/container_hash/hash.hpp>
#include <google/protobuf/arena.h>
#include <unordered_map>
#include <vector>
#include "substrait/VeloxToSubstraitType.h"
#include "velox/type/Type.h"
#include "velox/type/fbhive/HiveTypeParser.h"

namespace gluten {

class UdfLoader {
 public:
  struct UdfSignature {
    std::string name;
    std::string returnType;
    std::string argTypes;

    std::string intermediateType{};

    UdfSignature(std::string name, std::string returnType, std::string argTypes)
        : name(name), returnType(returnType), argTypes(argTypes) {}

    UdfSignature(std::string name, std::string returnType, std::string argTypes, std::string intermediateType)
        : name(name), returnType(returnType), argTypes(argTypes), intermediateType(intermediateType) {}

    ~UdfSignature() = default;
  };

  static std::shared_ptr<UdfLoader> getInstance();

  void loadUdfLibraries(const std::string& libPaths);

  std::unordered_set<std::shared_ptr<UdfSignature>> getRegisteredUdfSignatures();

  std::unordered_set<std::string> getRegisteredUdafNames();

  void registerUdf();

 private:
  void loadUdfLibraries0(const std::vector<std::string>& libPaths);

  std::string toSubstraitTypeStr(const std::string& type) {
    auto returnType = parser_.parse(type);
    auto substraitType = convertor_.toSubstraitType(arena_, returnType);

    std::string output;
    substraitType.SerializeToString(&output);
    return output;
  }

  std::string toSubstraitTypeStr(int32_t numArgs, const char** args) {
    std::vector<facebook::velox::TypePtr> argTypes;
    argTypes.resize(numArgs);
    for (auto i = 0; i < numArgs; ++i) {
      argTypes[i] = parser_.parse(args[i]);
    }
    auto substraitType = convertor_.toSubstraitType(arena_, facebook::velox::ROW(std::move(argTypes)));

    std::string output;
    substraitType.SerializeToString(&output);
    return output;
  }

  std::unordered_map<std::string, void*> handles_;

  facebook::velox::type::fbhive::HiveTypeParser parser_{};
  google::protobuf::Arena arena_{};
  VeloxToSubstraitTypeConvertor convertor_{};

  std::unordered_set<std::shared_ptr<UdfSignature>> signatures_;
  std::unordered_set<std::string> names_;
};
} // namespace gluten
