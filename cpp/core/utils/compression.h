// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <arrow/util/compression.h>

#ifdef GLUTEN_ENABLE_QAT
#include "utils/qat/qat_util.h"
#endif

#ifdef GLUTEN_ENABLE_IAA
#include "utils/qpl/qpl_codec.h"
#endif

namespace gluten {

#if defined(GLUTEN_ENABLE_QAT) or defined(GLUTEN_ENABLE_IAA)
static const std::vector<arrow::Compression::type> supported_codec = {
    arrow::Compression::LZ4_FRAME,
    arrow::Compression::ZSTD,
    arrow::Compression::CUSTOM};
#else
static const std::vector<arrow::Compression::type> supported_codec = {
    arrow::Compression::LZ4_FRAME,
    arrow::Compression::ZSTD};
#endif

arrow::Result<std::unique_ptr<arrow::util::Codec>> CreateArrowIpcCodec(arrow::Compression::type compressed_type) {
  if (std::any_of(supported_codec.begin(), supported_codec.end(), [&](const auto& codec) {
        return codec == compressed_type;
      })) {
    auto ret = arrow::util::Codec::Create(compressed_type);
    return ret;
  } else {
    return nullptr;
  }
}

} // namespace gluten
