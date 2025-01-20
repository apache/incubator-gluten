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

#include <arrow/result.h>
#include <arrow/util/compression.h>
#include <arrow/util/logging.h>
#include <qatseqprod.h>
#include <qatzip.h>
#include <zstd.h>
#include <algorithm>
#include <mutex>

#include "QatCodec.h"

#define QZ_INIT_FAIL(rc) ((QZ_OK != (rc)) && (QZ_DUPLICATE != (rc)))

#define QZ_SETUP_SESSION_FAIL(rc) (QZ_PARAMS == (rc) || QZ_NOSW_NO_HW == (rc) || QZ_NOSW_LOW_MEM == (rc))

namespace gluten {
namespace qat {

class QatZipCodec : public arrow::util::Codec {
 protected:
  explicit QatZipCodec(int compressionLevel) : compressionLevel_(compressionLevel) {}

  ~QatZipCodec() {
    static_cast<void>(qzTeardownSession(&qzSession_));
    static_cast<void>(qzClose(&qzSession_));
  }

  arrow::Result<int64_t> Decompress(int64_t inputLen, const uint8_t* input, int64_t outputLen, uint8_t* output)
      override {
    uint32_t compressedSize = static_cast<uint32_t>(inputLen);
    uint32_t uncompressedSize = static_cast<uint32_t>(outputLen);
    int ret = qzDecompress(&qzSession_, input, &compressedSize, output, &uncompressedSize);
    if (ret == QZ_OK) {
      return static_cast<int64_t>(uncompressedSize);
    } else if (ret == QZ_PARAMS) {
      return arrow::Status::IOError("QAT decompression failure: params is invalid");
    } else if (ret == QZ_FAIL) {
      return arrow::Status::IOError("QAT decompression failure: Function did not succeed");
    } else {
      return arrow::Status::IOError("QAT decompression failure with error:", ret);
    }
  }

  int64_t MaxCompressedLen(int64_t inputLen, const uint8_t* ARROW_ARG_UNUSED(input)) override {
    ARROW_DCHECK_GE(inputLen, 0);
    return qzMaxCompressedLength(static_cast<size_t>(inputLen), &qzSession_);
  }

  arrow::Result<int64_t> Compress(int64_t inputLen, const uint8_t* input, int64_t outputLen, uint8_t* output) override {
    uint32_t uncompressedSize = static_cast<uint32_t>(inputLen);
    uint32_t compressedSize = static_cast<uint32_t>(outputLen);
    int ret = qzCompress(&qzSession_, input, &uncompressedSize, output, &compressedSize, 1);
    if (ret == QZ_OK) {
      return static_cast<int64_t>(compressedSize);
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

  int compression_level() const override {
    return compressionLevel_;
  }

  int compressionLevel_;
  QzSession_T qzSession_ = {0};
};

class QatGZipCodec final : public QatZipCodec {
 public:
  QatGZipCodec(QzPollingMode_T pollingMode, int compressionLevel) : QatZipCodec(compressionLevel) {
    auto rc = qzInit(&qzSession_, /* sw_backup = */ 1);
    if (QZ_INIT_FAIL(rc)) {
      ARROW_LOG(WARNING) << "qzInit failed with error: " << rc;
    } else {
      QzSessionParamsDeflate_T params;
      qzGetDefaultsDeflate(&params); // get the default value.
      params.common_params.polling_mode = pollingMode;
      params.common_params.comp_lvl = compressionLevel;
      rc = qzSetupSessionDeflate(&qzSession_, &params);
      if (QZ_SETUP_SESSION_FAIL(rc)) {
        ARROW_LOG(WARNING) << "qzSetupSession failed with error: " << rc;
      }
    }
  }

  arrow::Compression::type compression_type() const override {
    return arrow::Compression::GZIP;
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
};

bool supportsCodec(const std::string& codec) {
  return !codec.empty() && std::any_of(kQatSupportedCodec.begin(), kQatSupportedCodec.end(), [&](const auto& qatCodec) {
    return qatCodec == codec;
  });
}

std::unique_ptr<arrow::util::Codec> makeQatGZipCodec(QzPollingMode_T pollingMode, int compressionLevel) {
  return std::unique_ptr<arrow::util::Codec>(new QatGZipCodec(pollingMode, compressionLevel));
}

std::unique_ptr<arrow::util::Codec> makeDefaultQatGZipCodec() {
  return makeQatGZipCodec(QZ_BUSY_POLLING, QZ_COMP_LEVEL_DEFAULT);
}

namespace {
constexpr int kZSTDDefaultCompressionLevel = 1;
arrow::Status ZSTDError(size_t ret, const char* prefix_msg) {
  return arrow::Status::IOError(prefix_msg, ZSTD_getErrorName(ret));
}

void logZstdOnError(size_t ret, const char* prefixMsg) {
  if (ZSTD_isError(ret)) {
    ARROW_LOG(WARNING) << prefixMsg << ZSTD_getErrorName(ret);
  }
}

} // namespace

class QatDevice {
 public:
  QatDevice() {
    if (QZSTD_startQatDevice() == QZSTD_FAIL) {
      ARROW_LOG(WARNING) << "QZSTD_startQatDevice failed";
    } else {
      initialized_ = true;
    }
  }

  ~QatDevice() {
    if (initialized_) {
      QZSTD_stopQatDevice();
    }
  }

  static std::shared_ptr<QatDevice> getInstance() {
    std::call_once(initQat_, []() { instance_ = std::make_shared<QatDevice>(); });
    return instance_;
  }

  bool deviceInitialized() {
    return initialized_;
  }

 private:
  inline static std::shared_ptr<QatDevice> instance_;
  inline static std::once_flag initQat_;
  bool initialized_{false};
};

class QatZstdCodec final : public arrow::util::Codec {
 public:
  explicit QatZstdCodec(int compressionLevel) : compressionLevel_(compressionLevel) {}

  ~QatZstdCodec() {
    if (initCCtx_) {
      ZSTD_freeCCtx(zc_);
      if (sequenceProducerState_) {
        QZSTD_freeSeqProdState(sequenceProducerState_);
      }
    }
  }

  arrow::Compression::type compression_type() const override {
    return arrow::Compression::ZSTD;
  }

  arrow::Result<int64_t> Decompress(int64_t inputLen, const uint8_t* input, int64_t outputLen, uint8_t* output)
      override {
    if (output == nullptr) {
      // We may pass a NULL 0-byte output buffer but some zstd versions demand
      // a valid pointer: https://github.com/facebook/zstd/issues/1385
      static uint8_t emptyBuffer;
      DCHECK_EQ(outputLen, 0);
      output = &emptyBuffer;
    }

    size_t ret = ZSTD_decompress(output, static_cast<size_t>(outputLen), input, static_cast<size_t>(inputLen));
    if (ZSTD_isError(ret)) {
      return ZSTDError(ret, "ZSTD decompression failed: ");
    }
    if (static_cast<int64_t>(ret) != outputLen) {
      return arrow::Status::IOError("Corrupt ZSTD compressed data.");
    }
    return static_cast<int64_t>(ret);
  }

  int64_t MaxCompressedLen(int64_t inputLen, const uint8_t* ARROW_ARG_UNUSED(input)) override {
    DCHECK_GE(inputLen, 0);
    return ZSTD_compressBound(static_cast<size_t>(inputLen));
  }

  arrow::Result<int64_t> Compress(int64_t inputLen, const uint8_t* input, int64_t outputLen, uint8_t* output) override {
    RETURN_NOT_OK(initCCtx());
    size_t ret = ZSTD_compress2(zc_, output, static_cast<size_t>(outputLen), input, static_cast<size_t>(inputLen));
    if (ZSTD_isError(ret)) {
      return ZSTDError(ret, "ZSTD compression failed: ");
    }
    return static_cast<int64_t>(ret);
  }

  arrow::Result<std::shared_ptr<arrow::util::Compressor>> MakeCompressor() override {
    return arrow::Status::NotImplemented("Streaming compression unsupported with QAT");
  }

  arrow::Result<std::shared_ptr<arrow::util::Decompressor>> MakeDecompressor() override {
    return arrow::Status::NotImplemented("Streaming decompression unsupported with QAT");
  }

  int minimum_compression_level() const override {
    return ZSTD_minCLevel();
  }
  int maximum_compression_level() const override {
    return ZSTD_maxCLevel();
  }
  int default_compression_level() const override {
    return kZSTDDefaultCompressionLevel;
  }

  int compression_level() const override {
    return compressionLevel_;
  }

 private:
  int compressionLevel_;
  ZSTD_CCtx* zc_;
  bool initCCtx_{false};

  std::shared_ptr<QatDevice> qatDevice_;
  void* sequenceProducerState_{nullptr};

  arrow::Status initCCtx() {
    if (initCCtx_) {
      return arrow::Status::OK();
    }
    zc_ = ZSTD_createCCtx();
    logZstdOnError(
        ZSTD_CCtx_setParameter(zc_, ZSTD_c_compressionLevel, compressionLevel_),
        "ZSTD_CCtx_setParameter failed on ZSTD_c_compressionLevel: ");
    if (!qatDevice_) {
      qatDevice_ = QatDevice::getInstance();
    }
    if (qatDevice_->deviceInitialized()) {
      sequenceProducerState_ = QZSTD_createSeqProdState();
      /* register qatSequenceProducer */
      ZSTD_registerSequenceProducer(zc_, sequenceProducerState_, qatSequenceProducer);
      /* Enable sequence producer fallback */
      logZstdOnError(
          ZSTD_CCtx_setParameter(zc_, ZSTD_c_enableSeqProducerFallback, 1),
          "ZSTD_CCtx_setParameter failed on  ZSTD_c_enableSeqProducerFallback: ");
    }
    initCCtx_ = true;
    return arrow::Status::OK();
  }
};

std::unique_ptr<arrow::util::Codec> makeQatZstdCodec(int compressionLevel) {
  return std::unique_ptr<arrow::util::Codec>(new QatZstdCodec(compressionLevel));
}

std::unique_ptr<arrow::util::Codec> makeDefaultQatZstdCodec() {
  return makeQatZstdCodec(kZSTDDefaultCompressionLevel);
}

} // namespace qat
} // namespace gluten
