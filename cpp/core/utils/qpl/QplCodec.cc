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

#include <arrow/util/compression.h>
#include <arrow/util/logging.h>
#include <utils/qpl/QplCodec.h>
#include <utils/qpl/QplJobPool.h>
#include <iostream>
#include <map>

namespace gluten {
namespace qpl {

class HardwareCodecDeflateQpl {
 public:
  /// RET_ERROR stands for hardware codec fail,need fallback to software codec.
  static constexpr int64_t RET_ERROR = -1;

  explicit HardwareCodecDeflateQpl(qpl_compression_levels compressionLevel) : compressionLevel_(compressionLevel){};

  int64_t doCompressData(const uint8_t* source, uint32_t source_size, uint8_t* dest, uint32_t dest_size) const {
    uint32_t job_id;
    qpl_job* jobPtr;
    if (!(jobPtr = QplJobHWPool::GetInstance().AcquireJob(job_id))) {
      ARROW_LOG(WARNING)
          << "DeflateQpl HW codec failed, falling back to SW codec. (Details: doCompressData->AcquireJob fail, probably job pool exhausted)";
      return RET_ERROR;
    }

    jobPtr->op = qpl_op_compress;
    jobPtr->next_in_ptr = const_cast<uint8_t*>(source);
    jobPtr->next_out_ptr = dest;
    jobPtr->available_in = source_size;
    jobPtr->level = compressionLevel_;
    jobPtr->available_out = dest_size;
    jobPtr->flags = QPL_FLAG_FIRST | QPL_FLAG_DYNAMIC_HUFFMAN | QPL_FLAG_LAST | QPL_FLAG_OMIT_VERIFY;

    if (auto status = qpl_execute_job(jobPtr); status == QPL_STS_OK) {
      auto compressed_size = jobPtr->total_out;
      QplJobHWPool::GetInstance().ReleaseJob(job_id);
      return compressed_size;
    } else {
      ARROW_LOG(WARNING)
          << "DeflateQpl HW codec failed, falling back to SW codec. (Details: doCompressData->qpl_execute_job with error code: "
          << status << " - please refer to qpl_status in ./contrib/qpl/include/qpl/c_api/status.h)";
      QplJobHWPool::GetInstance().ReleaseJob(job_id);
      return RET_ERROR;
    }
  }

  /// Submit job request to the IAA hardware and then busy waiting till it complete.
  int64_t doDecompressData(const uint8_t* source, uint32_t source_size, uint8_t* dest, uint32_t uncompressed_size) {
    uint32_t job_id = 0;
    qpl_job* jobPtr;
    if (!(jobPtr = QplJobHWPool::GetInstance().AcquireJob(job_id))) {
      ARROW_LOG(WARNING)
          << "DeflateQpl HW codec failed, falling back to SW codec.(Details: doDecompressData->AcquireJob fail, probably job pool exhausted)";
      return RET_ERROR;
    }

    // Performing a decompression operation
    jobPtr->op = qpl_op_decompress;
    jobPtr->next_in_ptr = const_cast<uint8_t*>(source);
    jobPtr->next_out_ptr = dest;
    jobPtr->available_in = source_size;
    jobPtr->available_out = uncompressed_size;
    jobPtr->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST;

    if (auto status = qpl_execute_job(jobPtr); status == QPL_STS_OK) {
      auto decompressed_size = jobPtr->total_out;
      QplJobHWPool::GetInstance().ReleaseJob(job_id);
      return decompressed_size;
    } else {
      ARROW_LOG(WARNING)
          << "DeflateQpl HW codec failed, falling back to SW codec. (Details: doDeCompressData->qpl_execute_job with error code: "
          << status << " - please refer to qpl_status in ./contrib/qpl/include/qpl/c_api/status.h)";
      QplJobHWPool::GetInstance().ReleaseJob(job_id);
      return RET_ERROR;
    }
  }

 private:
  qpl_compression_levels compressionLevel_ = qpl_default_level;
};

class SoftwareCodecDeflateQpl final {
 public:
  explicit SoftwareCodecDeflateQpl(qpl_compression_levels compressionLevel) : compressionLevel_(compressionLevel){};

  ~SoftwareCodecDeflateQpl() {
    if (swJob) {
      qpl_fini_job(swJob);
    }
  }

  int64_t doCompressData(const uint8_t* source, uint32_t source_size, uint8_t* dest, uint32_t dest_size) {
    qpl_job* jobPtr = getJobCodecPtr();
    // Performing a compression operation
    jobPtr->op = qpl_op_compress;
    jobPtr->next_in_ptr = const_cast<uint8_t*>(source);
    jobPtr->next_out_ptr = dest;
    jobPtr->available_in = source_size;
    jobPtr->level = compressionLevel_;
    jobPtr->available_out = dest_size;
    jobPtr->flags = QPL_FLAG_FIRST | QPL_FLAG_DYNAMIC_HUFFMAN | QPL_FLAG_LAST | QPL_FLAG_OMIT_VERIFY;

    if (auto status = qpl_execute_job(jobPtr); status != QPL_STS_OK) {
      throw GlutenException(
          "Execution of DeflateQpl software fallback codec failed. (Details: qpl_init_job with error code: " +
          std::to_string(status) + " - please refer to qpl_status in ./contrib/qpl/include/qpl/c_api/status.h)");
    }

    return jobPtr->total_out;
  }

  int64_t doDecompressData(const uint8_t* source, uint32_t source_size, uint8_t* dest, uint32_t uncompressed_size) {
    qpl_job* jobPtr = getJobCodecPtr();

    // Performing a decompression operation
    jobPtr->op = qpl_op_decompress;
    jobPtr->next_in_ptr = const_cast<uint8_t*>(source);
    jobPtr->next_out_ptr = dest;
    jobPtr->available_in = source_size;
    jobPtr->available_out = uncompressed_size;
    jobPtr->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST;

    if (auto status = qpl_execute_job(jobPtr); status != QPL_STS_OK) {
      throw GlutenException(
          "Execution of DeflateQpl software fallback codec failed. (Details: qpl_init_job with error code: " +
          std::to_string(status) + " - please refer to qpl_status in ./contrib/qpl/include/qpl/c_api/status.h)");
    }

    return jobPtr->total_out;
  }

 private:
  qpl_job* swJob = nullptr;
  std::unique_ptr<uint8_t[]> sw_buffer;
  qpl_compression_levels compressionLevel_ = qpl_default_level;

  qpl_job* getJobCodecPtr() {
    if (!swJob) {
      uint32_t size = 0;
      qpl_get_job_size(qpl_path_software, &size);

      sw_buffer = std::make_unique<uint8_t[]>(size);
      swJob = reinterpret_cast<qpl_job*>(sw_buffer.get());

      // Job initialization
      if (auto status = qpl_init_job(qpl_path_software, swJob); status != QPL_STS_OK)
        throw GlutenException(
            "Initialization of DeflateQpl software fallback codec failed. (Details: qpl_init_job with error code: " +
            std::to_string(status) + " - please refer to qpl_status in ./contrib/qpl/include/qpl/c_api/status.h)");
    }
    return swJob;
  }
};

class QplGzipCodec final : public arrow::util::Codec {
 public:
  explicit QplGzipCodec(qpl_compression_levels compressionLevel)
      : hwCodec_(std::make_unique<HardwareCodecDeflateQpl>(compressionLevel)),
        swCodec_(std::make_unique<SoftwareCodecDeflateQpl>(compressionLevel)) {}

  arrow::Result<int64_t>
  Compress(int64_t input_len, const uint8_t* input, int64_t output_buffer_len, uint8_t* output_buffer) override {
    auto res = HardwareCodecDeflateQpl::RET_ERROR;
    if (QplJobHWPool::GetInstance().IsJobPoolReady()) {
      res = hwCodec_->doCompressData(input, input_len, output_buffer, output_buffer_len);
    }
    if (res == HardwareCodecDeflateQpl::RET_ERROR) {
      return swCodec_->doCompressData(input, input_len, output_buffer, output_buffer_len);
    }
    return res;
  }

  arrow::Result<int64_t>
  Decompress(int64_t input_len, const uint8_t* input, int64_t output_buffer_len, uint8_t* output_buffer) override {
    auto res = HardwareCodecDeflateQpl::RET_ERROR;
    if (QplJobHWPool::GetInstance().IsJobPoolReady()) {
      res = hwCodec_->doDecompressData(input, input_len, output_buffer, output_buffer_len);
    }
    if (res == HardwareCodecDeflateQpl::RET_ERROR) {
      return swCodec_->doDecompressData(input, input_len, output_buffer, output_buffer_len);
    }
    return res;
  }

  int64_t MaxCompressedLen(int64_t input_len, const uint8_t* ARROW_ARG_UNUSED(input)) override {
    ARROW_DCHECK_GE(input_len, 0);
    /// Aligned with ZLIB
    return ((input_len) + ((input_len) >> 12) + ((input_len) >> 14) + ((input_len) >> 25) + 13LL);
  }

  arrow::Result<std::shared_ptr<arrow::util::Compressor>> MakeCompressor() override {
    return arrow::Status::NotImplemented("Streaming compression unsupported with QAT");
  }

  arrow::Result<std::shared_ptr<arrow::util::Decompressor>> MakeDecompressor() override {
    return arrow::Status::NotImplemented("Streaming decompression unsupported with QAT");
  }

  arrow::Compression::type compression_type() const override {
    return arrow::Compression::GZIP;
  }

  int minimum_compression_level() const override {
    return qpl_level_1;
  }
  int maximum_compression_level() const override {
    return qpl_high_level;
  }
  int default_compression_level() const override {
    return qpl_default_level;
  }

 private:
  std::unique_ptr<HardwareCodecDeflateQpl> hwCodec_;
  std::unique_ptr<SoftwareCodecDeflateQpl> swCodec_;
};

bool SupportsCodec(const std::string& codec) {
  if (std::any_of(qpl_supported_codec.begin(), qpl_supported_codec.end(), [&](const auto& qat_codec) {
        return qat_codec == codec;
      })) {
    return true;
  }
  return false;
}

std::unique_ptr<arrow::util::Codec> MakeQplGZipCodec(int compressionLevel) {
  auto qplCompressionLevel = static_cast<qpl_compression_levels>(compressionLevel);
  return std::unique_ptr<arrow::util::Codec>(new QplGzipCodec(qplCompressionLevel));
}

std::unique_ptr<arrow::util::Codec> MakeDefaultQplGZipCodec() {
  return MakeQplGZipCodec(qpl_default_level);
}

} // namespace qpl
} // namespace gluten
