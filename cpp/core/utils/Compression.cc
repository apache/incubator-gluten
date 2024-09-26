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

#include "utils/Compression.h"

#include "Exception.h"

#ifdef GLUTEN_ENABLE_QAT
#include "utils/qat/QatCodec.h"
#endif

#ifdef GLUTEN_ENABLE_IAA
#include "utils/qpl/QplCodec.h"
#endif

namespace gluten {

std::unique_ptr<arrow::util::Codec>
createArrowIpcCodec(arrow::Compression::type compressedType, CodecBackend codecBackend, int32_t compressionLevel) {
  std::unique_ptr<arrow::util::Codec> codec;
  switch (compressedType) {
    case arrow::Compression::LZ4_FRAME: {
      GLUTEN_ASSIGN_OR_THROW(codec, arrow::util::Codec::Create(compressedType));
    } break;
    case arrow::Compression::ZSTD: {
      if (codecBackend == CodecBackend::NONE) {
        GLUTEN_ASSIGN_OR_THROW(codec, arrow::util::Codec::Create(compressedType, compressionLevel));
      } else if (codecBackend == CodecBackend::QAT) {
#if defined(GLUTEN_ENABLE_QAT)
        codec = qat::makeDefaultQatZstdCodec();
#else
        throw GlutenException("Backend QAT but not compile with option GLUTEN_ENABLE_QAT");
#endif
      } else {
        throw GlutenException("Backend IAA not support zstd compression");
      }
    } break;
    case arrow::Compression::GZIP: {
      if (codecBackend == CodecBackend::NONE) {
        return nullptr;
      } else if (codecBackend == CodecBackend::QAT) {
#if defined(GLUTEN_ENABLE_QAT)
        codec = qat::makeDefaultQatGZipCodec();
#else
        throw GlutenException("Backend QAT but not compile with option GLUTEN_ENABLE_QAT");
#endif
      } else {
#if defined(GLUTEN_ENABLE_IAA)
        codec = qpl::MakeDefaultQplGZipCodec();
#else
        throw GlutenException("Backend IAA but not compile with option GLUTEN_ENABLE_IAA");
#endif
      }
    } break;
    default:
      return nullptr;
  }
  return codec;
}
} // namespace gluten
