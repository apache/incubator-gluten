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

#include "Common.h"

namespace gluten {

std::unique_ptr<re2::RE2> compilePattern(const std::string& pattern) {
  return std::make_unique<re2::RE2>(re2::StringPiece(pattern), RE2::Quiet);
}

bool validatePattern(const std::string& pattern, std::string& error) {
  auto re2 = compilePattern(pattern);
  if (!re2->ok()) {
    error =
        "native validation failed due to: pattern " + pattern + " compilation failed in RE2. Reason: " + re2->error();
    return false;
  }
  return true;
}

} // namespace gluten
