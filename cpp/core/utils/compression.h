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

#include <arrow/util/compression.h>
#include <bits/stl_algo.h>
#include <vector>

#include "exception.h"

#ifdef GLUTEN_ENABLE_QAT
#include "utils/qat/QatCodec.h"
#endif

#ifdef GLUTEN_ENABLE_IAA
#include "utils/qpl/qpl_codec.h"
#endif

namespace gluten {

enum CodecBackend { NONE, QAT, IAA };

#if defined(GLUTEN_ENABLE_QAT)
static const std::vector<arrow::Compression::type> kSupportedCodec = {
    arrow::Compression::LZ4_FRAME,
    arrow::Compression::ZSTD,
    arrow::Compression::GZIP};
inline Result<std::unique_ptr<arrow::util::Codec>> createCodec(
    arrow::Compression::type compressedType,
    CodecBackend codecBackend) {
  std::unique_ptr<arrow::util::Codec> codec;
  switch (compressedType) {
    case arrow::Compression::LZ4_FRAME:
      return arrow::util::Codec::Create(compressedType);
    case arrow::Compression::ZSTD: {
      if (codecBackend == CodecBackend::NONE) {
        return arrow::util::Codec::Create(compressedType);
      }
      codec = qat::makeDefaultQatZstdCodec();
    } break;
    case arrow::Compression::GZIP: {
      if (codecBackend == CodecBackend::NONE) {
        return arrow::util::Codec::Create(compressedType);
      }
      codec = qat::makeDefaultQatGZipCodec();
    } break;
    default:
      return nullptr;
  }
  return std::move(codec);
}
#elif defined(GLUTEN_ENABLE_IAA)
static const std::vector<arrow::Compression::type> kSupportedCodec = {
    arrow::Compression::LZ4_FRAME,
    arrow::Compression::ZSTD,
    arrow::Compression::GZIP};
inline Result<std::unique_ptr<arrow::util::Codec>> createCodec(
    arrow::Compression::type compressedType,
    CodecBackend codecBackend) {
  std::unique_ptr<arrow::util::Codec> codec;
  switch (compressedType) {
    case arrow::Compression::LZ4_FRAME:
    case arrow::Compression::ZSTD:
      return arrow::util::Codec::Create(compressedType);
    case arrow::Compression::GZIP: {
      if (codecBackend == CodecBackend::NONE) {
        return arrow::util::Codec::Create(compressedType);
      }
      codec = qpl::MakeDefaultQplGZipCodec();
    } break;
    default:
      return nullptr;
  }
  return std::move(codec);
}
#else
static const std::vector<arrow::Compression::type> kSupportedCodec = {
    arrow::Compression::LZ4_FRAME,
    arrow::Compression::ZSTD};
#endif

inline std::unique_ptr<arrow::util::Codec> createArrowIpcCodec(
    arrow::Compression::type compressedType,
    CodecBackend codecBackend) {
  if (std::any_of(kSupportedCodec.begin(), kSupportedCodec.end(), [compressedType](const auto& codec) {
        return codec == compressedType;
      })) {
#if defined(GLUTEN_ENABLE_QAT) or defined(GLUTEN_ENABLE_IAA)
    GLUTEN_ASSIGN_OR_THROW(auto ret, createCodec(compressedType, codecBackend));
#else
    GLUTEN_ASSIGN_OR_THROW(auto ret, arrow::util::Codec::Create(compressedType));
#endif
    return ret;
  } else {
    return nullptr;
  }
}

} // namespace gluten
