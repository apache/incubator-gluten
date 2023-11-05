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

#include "shuffle/Utils.h"
#include "Options.h"
#include "utils/StringUtil.h"

#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <fcntl.h>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <sstream>
#include <thread>

std::string gluten::generateUuid() {
  boost::uuids::random_generator generator;
  return boost::uuids::to_string(generator());
}

std::string gluten::getSpilledShuffleFileDir(const std::string& configuredDir, int32_t subDirId) {
  auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
  std::stringstream ss;
  ss << std::setfill('0') << std::setw(2) << std::hex << subDirId;
  auto dir = arrow::fs::internal::ConcatAbstractPath(configuredDir, ss.str());
  return dir;
}

arrow::Result<std::string> gluten::createTempShuffleFile(const std::string& dir) {
  if (dir.length() == 0) {
    return arrow::Status::Invalid("Failed to create spilled file, got empty path.");
  }

  auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
  ARROW_ASSIGN_OR_RAISE(auto path_info, fs->GetFileInfo(dir));
  if (path_info.type() == arrow::fs::FileType::NotFound) {
    RETURN_NOT_OK(fs->CreateDir(dir, true));
  }

  bool exist = true;
  std::string filePath;
  while (exist) {
    filePath = arrow::fs::internal::ConcatAbstractPath(dir, "temp_shuffle_" + generateUuid());
    ARROW_ASSIGN_OR_RAISE(auto file_info, fs->GetFileInfo(filePath));
    if (file_info.type() == arrow::fs::FileType::NotFound) {
      int fd = open(filePath.c_str(), O_CREAT | O_EXCL | O_RDWR, 0666);
      if (fd < 0) {
        if (errno != EEXIST) {
          return arrow::Status::IOError("Failed to open local file " + filePath + ", Reason: " + strerror(errno));
        }
      } else {
        exist = false;
        close(fd);
      }
    }
  }
  return filePath;
}

arrow::Result<std::vector<std::shared_ptr<arrow::DataType>>> gluten::toShuffleWriterTypeId(
    const std::vector<std::shared_ptr<arrow::Field>>& fields) {
  std::vector<std::shared_ptr<arrow::DataType>> shuffleWriterTypeId;
  std::pair<std::string, arrow::Type::type> fieldTypeNotImplemented;
  for (auto field : fields) {
    switch (field->type()->id()) {
      case arrow::BooleanType::type_id:
      case arrow::Int8Type::type_id:
      case arrow::UInt8Type::type_id:
      case arrow::Int16Type::type_id:
      case arrow::UInt16Type::type_id:
      case arrow::HalfFloatType::type_id:
      case arrow::Int32Type::type_id:
      case arrow::UInt32Type::type_id:
      case arrow::FloatType::type_id:
      case arrow::Date32Type::type_id:
      case arrow::Time32Type::type_id:
      case arrow::Int64Type::type_id:
      case arrow::UInt64Type::type_id:
      case arrow::DoubleType::type_id:
      case arrow::Date64Type::type_id:
      case arrow::Time64Type::type_id:
      case arrow::TimestampType::type_id:
      case arrow::BinaryType::type_id:
      case arrow::StringType::type_id:
      case arrow::LargeBinaryType::type_id:
      case arrow::LargeStringType::type_id:
      case arrow::StructType::type_id:
      case arrow::MapType::type_id:
      case arrow::ListType::type_id:
      case arrow::LargeListType::type_id:
      case arrow::Decimal128Type::type_id:
      case arrow::NullType::type_id:
        shuffleWriterTypeId.push_back(field->type());
        break;
      default:
        RETURN_NOT_OK(arrow::Status::NotImplemented(
            "Field type not implemented in ColumnarShuffle, type is ", field->type()->ToString()));
    }
  }
  return shuffleWriterTypeId;
}

int64_t gluten::getBufferSizes(const std::shared_ptr<arrow::Array>& array) {
  return gluten::getBufferSizes(array->data()->buffers);
}

int64_t gluten::getBufferSizes(const std::vector<std::shared_ptr<arrow::Buffer>>& buffers) {
  return std::accumulate(
      std::cbegin(buffers), std::cend(buffers), 0LL, [](int64_t sum, const std::shared_ptr<arrow::Buffer>& buf) {
        return buf == nullptr ? sum : sum + buf->size();
      });
}

arrow::Status gluten::writeEos(arrow::io::OutputStream* os, int64_t* bytes) {
  // write EOS
  static constexpr int32_t kIpcContinuationToken = -1;
  static constexpr int32_t kZeroLength = 0;
  static const int64_t kSizeOfEos = sizeof(kIpcContinuationToken) + sizeof(kZeroLength);
  RETURN_NOT_OK(os->Write(&kIpcContinuationToken, sizeof(kIpcContinuationToken)));
  RETURN_NOT_OK(os->Write(&kZeroLength, sizeof(kZeroLength)));
  *bytes = kSizeOfEos;
  return arrow::Status::OK();
}
