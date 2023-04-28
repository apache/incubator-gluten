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

#include <arrow/result.h>
#include <arrow/util/compression.h>
#include <arrow/util/logging.h>
#include <qatzip.h>

#include "qat_util.h"

#define QZ_INIT_FAIL(rc) (QZ_OK != rc && QZ_DUPLICATE != rc)

#define QZ_SETUP_SESSION_FAIL(rc) (QZ_PARAMS == rc || QZ_NOSW_NO_HW == rc || QZ_NOSW_LOW_MEM == rc)

namespace gluten {
namespace qat {

class QatCodec : public arrow::util::Codec {
 protected:
  explicit QatCodec(QzPollingMode_T polling_mode) : pollingMode(polling_mode) {}

  arrow::Result<int64_t>
  Decompress(int64_t input_len, const uint8_t* input, int64_t output_buffer_len, uint8_t* output_buffer) override {
    uint32_t compressed_size = static_cast<uint32_t>(input_len);
    uint32_t uncompressed_size = static_cast<uint32_t>(output_buffer_len);
    int ret = qzDecompress(&qzSession, input, &compressed_size, output_buffer, &uncompressed_size);
    if (ret == QZ_OK) {
      return static_cast<int64_t>(uncompressed_size);
    } else if (ret == QZ_PARAMS) {
      return arrow::Status::IOError("QAT decompression failure: params is invalid");
    } else if (ret == QZ_FAIL) {
      return arrow::Status::IOError("QAT decompression failure: Function did not succeed");
    } else {
      return arrow::Status::IOError("QAT decompression failure with error:", ret);
    }
  }

  int64_t MaxCompressedLen(int64_t input_len, const uint8_t* ARROW_ARG_UNUSED(input)) override {
    ARROW_DCHECK_GE(input_len, 0);
    return qzMaxCompressedLength(static_cast<size_t>(input_len), &qzSession);
  }

  arrow::Result<int64_t>
  Compress(int64_t input_len, const uint8_t* input, int64_t output_buffer_len, uint8_t* output_buffer) override {
    uint32_t uncompressed_size = static_cast<uint32_t>(input_len);
    uint32_t compressed_size = static_cast<uint32_t>(output_buffer_len);
    int ret = qzCompress(&qzSession, input, &uncompressed_size, output_buffer, &compressed_size, 1);
    if (ret == QZ_OK) {
      return static_cast<int64_t>(compressed_size);
    } else if (ret == QZ_PARAMS) {
      return arrow::Status::IOError("QAT compression failure: params is invalid");
    } else if (ret == QZ_FAIL) {
      return arrow::Status::IOError("QAT compression failure: function did not succeed");
    } else {
      return arrow::Status::IOError("QAT compression failure with error:", ret);
    }
  }

  arrow::Result<std::shared_ptr<arrow::util::Compressor>> MakeCompressor() override {
    return arrow::Status::NotImplemented("Streaming compression unsupported with QAT");
  }

  arrow::Result<std::shared_ptr<arrow::util::Decompressor>> MakeDecompressor() override {
    return arrow::Status::NotImplemented("Streaming decompression unsupported with QAT");
  }

  QzSession_T qzSession = {0};
  QzPollingMode_T pollingMode;
};

class QatGZipCodec final : public QatCodec {
 public:
  QatGZipCodec(QzPollingMode_T polling_mode, int compression_level)
      : QatCodec(polling_mode), compressionLevel(compression_level) {
    auto rc = qzInit(&qzSession, 1);
    if (QZ_INIT_FAIL(rc)) {
      ARROW_LOG(WARNING) << "qzInit failed with error: " << rc;
    } else {
      QzSessionParamsDeflate_T params;
      qzGetDefaultsDeflate(&params); // get the default value.
      params.common_params.polling_mode = pollingMode;
      rc = qzSetupSessionDeflate(&qzSession, &params);
      if (QZ_SETUP_SESSION_FAIL(rc)) {
        ARROW_LOG(WARNING) << "qzSetupSession failed with error: " << rc;
      }
    }
  }

  arrow::Compression::type compression_type() const override {
    return arrow::Compression::CUSTOM;
  }

  int minimum_compression_level() const override {
    return QZ_DEFLATE_COMP_LVL_MINIMUM;
  }
  int maximum_compression_level() const override {
    return QZ_DEFLATE_COMP_LVL_MAXIMUM;
  }
  int default_compression_level() const override {
    return QZ_COMP_LEVEL_DEFAULT;
  }

 private:
  int compressionLevel; // unused.
};

bool SupportsCodec(const std::string& codec) {
  if (std::any_of(qat_supported_codec.begin(), qat_supported_codec.end(), [&](const auto& qat_codec) {
        return qat_codec == codec;
      })) {
    return true;
  }
  return false;
}

void EnsureQatCodecRegistered(const std::string& codec) {
  if (codec == "gzip") {
    arrow::util::RegisterCustomCodec([](int) { return MakeDefaultQatGZipCodec(); });
  }
}

std::unique_ptr<arrow::util::Codec> MakeQatGZipCodec(QzPollingMode_T polling_mode, int compression_level) {
  return std::unique_ptr<arrow::util::Codec>(new QatGZipCodec(polling_mode, compression_level));
}

std::unique_ptr<arrow::util::Codec> MakeDefaultQatGZipCodec() {
  return MakeQatGZipCodec(QZ_BUSY_POLLING, QZ_COMP_LEVEL_DEFAULT);
}

} // namespace qat
} // namespace gluten
