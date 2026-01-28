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

#include <cstddef>
#include <cstring>
#include <string>
#include <string_view>

#include "velox/common/base/Exceptions.h"
#include "velox/functions/Macros.h"
#include "velox/type/StringView.h"

namespace gluten {

template <typename T>
struct CastStringToBooleanAnsiFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<bool>& result,
      const arg_type<facebook::velox::Varchar>& input) {
    const auto trimmed = trimAsciiWhitespace(input);
    if (isTrueString(trimmed)) {
      result = true;
      return;
    }
    if (isFalseString(trimmed)) {
      result = false;
      return;
    }

    VELOX_USER_FAIL(
        "Invalid input syntax for boolean: '{}'",
        std::string(input.data(), input.size()));
  }

 private:
  struct TrimmedView {
    const char* data;
    size_t size;
  };

  static FOLLY_ALWAYS_INLINE bool isAsciiWhitespace(char value) {
    return static_cast<unsigned char>(value) <= 0x20;
  }

  static TrimmedView trimAsciiWhitespace(
      const arg_type<facebook::velox::Varchar>& input) {
    const char* data = input.data();
    size_t size = input.size();
    size_t start = 0;
    size_t end = size;
    while (start < end && isAsciiWhitespace(data[start])) {
      ++start;
    }
    while (end > start && isAsciiWhitespace(data[end - 1])) {
      --end;
    }
    return TrimmedView{data + start, end - start};
  }

  static bool equalsIgnoreCase(const TrimmedView& input, const char* literal) {
    const size_t literalSize = std::strlen(literal);
    if (input.size != literalSize) {
      return false;
    }
    for (size_t i = 0; i < literalSize; ++i) {
      char c = input.data[i];
      if (c >= 'A' && c <= 'Z') {
        c = static_cast<char>(c + ('a' - 'A'));
      }
      if (c != literal[i]) {
        return false;
      }
    }
    return true;
  }

  static bool isTrueString(const TrimmedView& input) {
    return equalsIgnoreCase(input, "t") || equalsIgnoreCase(input, "true") ||
        equalsIgnoreCase(input, "y") || equalsIgnoreCase(input, "yes") ||
        equalsIgnoreCase(input, "1");
  }

  static bool isFalseString(const TrimmedView& input) {
    return equalsIgnoreCase(input, "f") || equalsIgnoreCase(input, "false") ||
        equalsIgnoreCase(input, "n") || equalsIgnoreCase(input, "no") ||
        equalsIgnoreCase(input, "0");
  }
};

} // namespace gluten
