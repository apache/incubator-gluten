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

#include <filesystem>
#include <iostream>
#include <string_view>
#include <vector>

#include "StringUtil.h"
#include "exception.h"

std::vector<std::string> gluten::splitByDelim(const std::string& s, const char delimiter) {
  if (s.empty()) {
    return {};
  }
  std::vector<std::string> result;
  size_t start = 0;
  size_t end = s.find(delimiter);

  while (end != std::string::npos) {
    result.push_back(std::string(s.substr(start, end - start)));
    start = end + 1;
    end = s.find(delimiter, start);
  }

  result.push_back(std::string(s.substr(start)));
  return result;
}

std::vector<std::string> gluten::splitPaths(const std::string& s, bool checkExists) {
  if (s.empty()) {
    return {};
  }
  auto splits = splitByDelim(s, ',');
  std::vector<std::string> paths;
  for (auto i = 0; i < splits.size(); ++i) {
    if (!splits[i].empty()) {
      std::filesystem::path path(splits[i]);
      if (checkExists && !std::filesystem::exists(path)) {
        throw gluten::GlutenException("File path not exists: " + splits[i]);
      }
      if (path.is_relative()) {
        path = std::filesystem::current_path() / path;
      }
      paths.push_back(path.lexically_normal().generic_string());
    }
  }
  return paths;
}
