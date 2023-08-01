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
#include <zstd.h>
#include <iostream>
#include <vector>

#include "exception.h"

#ifdef GLUTEN_ENABLE_QAT
#include "utils/qat/QatCodec.h"
#endif

#ifdef GLUTEN_ENABLE_IAA
#include "utils/qpl/qpl_codec.h"
#endif

namespace gluten {

#if defined(GLUTEN_ENABLE_QAT) or defined(GLUTEN_ENABLE_IAA)
static const std::vector<arrow::Compression::type> kSupportedCodec = {
    arrow::Compression::LZ4_FRAME,
    arrow::Compression::ZSTD,
    arrow::Compression::CUSTOM};
#else
static const std::vector<arrow::Compression::type> kSupportedCodec = {
    arrow::Compression::LZ4_FRAME,
    arrow::Compression::ZSTD};
#endif

inline std::unique_ptr<arrow::util::Codec> createArrowIpcCodec(arrow::Compression::type compressedType) {
  if (std::any_of(kSupportedCodec.begin(), kSupportedCodec.end(), [compressedType](const auto& codec) {
        return codec == compressedType;
      })) {
    GLUTEN_ASSIGN_OR_THROW(auto ret, arrow::util::Codec::Create(compressedType));
    return ret;
  } else {
    return nullptr;
  }
}

inline void zstdUncompress(arrow::Buffer* compressed, arrow::ResizableBuffer* uncompressed) {
  GLUTEN_DCHECK(compressed != nullptr && compressed->size() > 0, "compressedBuffer cannot be empty");
  ZSTD_DStream* const cstream = ZSTD_createDStream();
  GLUTEN_CHECK(cstream != nullptr, "ZSTD_createDStream() cannot be null");

  size_t const initResult = ZSTD_initDStream(cstream);
  GLUTEN_CHECK(!ZSTD_isError(initResult), "ZSTD_initDStream() error: " + std::string(ZSTD_getErrorName(initResult)));

  ZSTD_inBuffer input = {compressed->data(), (size_t)compressed->size(), 0};
  ZSTD_outBuffer output = {NULL, 0, 0};
  while (input.pos < input.size) {
    output.dst = uncompressed->mutable_data() + uncompressed->size(); // current position in output buffer
    output.size = uncompressed->capacity() - uncompressed->size(); // remaining capacity in output buffer
    output.pos = 0;

    size_t const ret = ZSTD_decompressStream(cstream, &output, &input);
    GLUTEN_CHECK(!ZSTD_isError(initResult), "ZSTD_decompressStream() error: " + std::string(ZSTD_getErrorName(ret)));

    GLUTEN_THROW_NOT_OK(uncompressed->Resize(
        uncompressed->size() + output.pos, /*shrink_to_fit*/ false)); // update size of compressed buffer
    GLUTEN_CHECK(uncompressed->size() <= uncompressed->capacity(), "Should allocate buffer first");
  }

  // Will allocate the exact output buffer size.
  GLUTEN_CHECK(output.pos <= output.size, "Not all data was decompressed");
  ZSTD_freeDStream(cstream);
}

inline int64_t zstdMaxCompressedLength(int64_t uncompressedLength) {
  auto maxCompressedLen = ZSTD_compressBound(uncompressedLength);
  return maxCompressedLen;
}

inline void zstdCompressBuffers(
    const std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    arrow::ResizableBuffer* compressed,
    int32_t compressionLevel = 1) {
  ZSTD_CStream* const cstream = ZSTD_createCStream();
  GLUTEN_CHECK(cstream != nullptr, "ZSTD_createCStream() cannot be null");

  size_t const initResult = ZSTD_initCStream(cstream, compressionLevel);
  GLUTEN_CHECK(!ZSTD_isError(initResult), "ZSTD_initCStream() error:");
  // GLUTEN_CHECK(!ZSTD_isError(initResult), folly::format("ZSTD_initCStream() error: {}",
  // ZSTD_getErrorName(initResult)));

  // std::vector<uint8_t> compressed;
  ZSTD_outBuffer output = {NULL, 0, 0};

  // auto totalBufferLen =
  //     std::accumulate(buffers.begin(), buffers.end(), 0, [](int64_t sum, const std::shared_ptr<arrow::Buffer>&
  //     buffer) {
  //       return sum + buffer->size();
  //     });
  // auto maxCompressedLen = ZSTD_compressBound((size_t)totalBufferLen);
  // compressed->reserve(maxCompressedLen);

  for (const auto& buffer : buffers) {
    // if (buffer == nullptr || buffer->size() == 0) {
    //   continue;
    // }
    const uint8_t* bufferData = (buffer == nullptr || buffer->size() == 0) ? nullptr : buffer->data();
    size_t bufferSize = (buffer == nullptr || buffer->size() == 0) ? 0 : buffer->size();
    ZSTD_inBuffer input = {bufferData, bufferSize, 0};

    while (input.pos < input.size) {
      output.dst = compressed->mutable_data() + compressed->size(); // current position in output buffer
      output.size = compressed->capacity() - compressed->size(); // remaining capacity in output buffer
      output.pos = 0;

      size_t const ret = ZSTD_compressStream(cstream, &output, &input);
      if (ZSTD_isError(ret)) {
        std::cerr << "ZSTD_compressStream() error: " << ZSTD_getErrorName(ret) << std::endl;
        exit(1);
      }

      GLUTEN_THROW_NOT_OK(compressed->Resize(
          compressed->size() + output.pos, /*shrink_to_fit*/ false)); // update size of compressed buffer
      GLUTEN_CHECK(compressed->size() <= compressed->capacity(), "Should allocate buffer first");
      // if (compressed->size() == compressed->capacity()) { // resize output buffer if it's full
      //   compressed->Reserve(compressed->capacity() * 2);
      // }
    }
  }

  // flush any remaining data
  output.dst = compressed->mutable_data() + compressed->size();
  output.size = compressed->capacity() - compressed->size();
  output.pos = 0;

  size_t const remaining = ZSTD_endStream(cstream, &output);
  GLUTEN_CHECK(remaining == 0, "Not all data was flushed");

  GLUTEN_THROW_NOT_OK(
      compressed->Resize(compressed->size() + output.pos, /*shrink_to_fit*/ true)); // final size of compressed buffer

  ZSTD_freeCStream(cstream);
}

} // namespace gluten
