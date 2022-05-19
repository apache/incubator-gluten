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

#include <arrow/builder.h>
#include <arrow/pretty_print.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <arrow/io/memory.h>
#include <arrow/util/parallel.h>
#include <gandiva/arrow.h>
#include <gandiva/gandiva_aliases.h>
#include <gandiva/tree_expr_builder.h>
#include <google/protobuf/io/coded_stream.h>

#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "compute/protobuf_utils.h"
#include "compute/substrait_utils.h"

jclass CreateGlobalClassReference(JNIEnv* env, const char* class_name) {
  jclass local_class = env->FindClass(class_name);
  jclass global_class = (jclass)env->NewGlobalRef(local_class);
  env->DeleteLocalRef(local_class);
  return global_class;
}

jmethodID GetMethodID(JNIEnv* env, jclass this_class, const char* name, const char* sig) {
  jmethodID ret = env->GetMethodID(this_class, name, sig);
  return ret;
}

jmethodID GetStaticMethodID(JNIEnv* env, jclass this_class, const char* name,
                            const char* sig) {
  jmethodID ret = env->GetStaticMethodID(this_class, name, sig);
  return ret;
}

std::shared_ptr<arrow::DataType> GetOffsetDataType(
    std::shared_ptr<arrow::DataType> parent_type) {
  switch (parent_type->id()) {
    case arrow::BinaryType::type_id:
      return std::make_shared<arrow::TypeTraits<arrow::BinaryType>::OffsetType>();
    case arrow::LargeBinaryType::type_id:
      return std::make_shared<arrow::TypeTraits<arrow::LargeBinaryType>::OffsetType>();
    case arrow::ListType::type_id:
      return std::make_shared<arrow::TypeTraits<arrow::ListType>::OffsetType>();
    case arrow::LargeListType::type_id:
      return std::make_shared<arrow::TypeTraits<arrow::LargeListType>::OffsetType>();
    default:
      return nullptr;
  }
}

template <typename T>
bool is_fixed_width_type(T _) {
  return std::is_base_of<arrow::FixedWidthType, T>::value;
}

arrow::Status AppendNodes(std::shared_ptr<arrow::Array> column,
                          std::vector<std::pair<int64_t, int64_t>>* nodes) {
  auto type = column->type();
  (*nodes).push_back(std::make_pair(column->length(), column->null_count()));
  switch (type->id()) {
    case arrow::Type::LIST:
    case arrow::Type::LARGE_LIST: {
      auto list_array = std::dynamic_pointer_cast<arrow::ListArray>(column);
      RETURN_NOT_OK(AppendNodes(list_array->values(), nodes));
    } break;
    default: {
    } break;
  }
  return arrow::Status::OK();
}

arrow::Status AppendBuffers(std::shared_ptr<arrow::Array> column,
                            std::vector<std::shared_ptr<arrow::Buffer>>* buffers) {
  auto type = column->type();
  switch (type->id()) {
    case arrow::Type::LIST:
    case arrow::Type::LARGE_LIST: {
      auto list_array = std::dynamic_pointer_cast<arrow::ListArray>(column);
      (*buffers).push_back(list_array->null_bitmap());
      (*buffers).push_back(list_array->value_offsets());
      RETURN_NOT_OK(AppendBuffers(list_array->values(), buffers));
    } break;
    default: {
      for (auto& buffer : column->data()->buffers) {
        (*buffers).push_back(buffer);
      }
    } break;
  }
  return arrow::Status::OK();
}

arrow::Status FIXOffsetBuffer(std::shared_ptr<arrow::Buffer>* in_buf, int fix_row) {
  if ((*in_buf) == nullptr || (*in_buf)->size() == 0) return arrow::Status::OK();
  if ((*in_buf)->size() * 8 <= fix_row) {
    ARROW_ASSIGN_OR_RAISE(auto valid_copy, arrow::AllocateBuffer((*in_buf)->size() + 1));
    std::memcpy(valid_copy->mutable_data(), (*in_buf)->data(),
                static_cast<size_t>((*in_buf)->size()));
    (*in_buf) = std::move(valid_copy);
  }
  arrow::bit_util::SetBitsTo(const_cast<uint8_t*>((*in_buf)->data()), fix_row, 1, true);
  return arrow::Status::OK();
}

arrow::Status MakeArrayData(std::shared_ptr<arrow::DataType> type, int num_rows,
                            std::vector<std::shared_ptr<arrow::Buffer>> in_bufs,
                            int in_bufs_len, std::shared_ptr<arrow::ArrayData>* arr_data,
                            int* buf_idx_ptr) {
  if (arrow::is_nested(type->id())) {
    // Maybe ListType, MapType, StructType or UnionType
    switch (type->id()) {
      case arrow::Type::LIST:
      case arrow::Type::LARGE_LIST: {
        auto offset_data_type = GetOffsetDataType(type);
        auto list_type = std::dynamic_pointer_cast<arrow::ListType>(type);
        auto child_type = list_type->value_type();
        std::shared_ptr<arrow::ArrayData> child_array_data, offset_array_data;
        // create offset array
        // Chendi: For some reason, for ListArray::FromArrays will remove last row from
        // offset array, refer to array_nested.cc CleanListOffsets function
        FIXOffsetBuffer(&in_bufs[*buf_idx_ptr], num_rows);
        RETURN_NOT_OK(MakeArrayData(offset_data_type, num_rows + 1, in_bufs, in_bufs_len,
                                    &offset_array_data, buf_idx_ptr));
        auto offset_array = arrow::MakeArray(offset_array_data);
        // create child data array
        RETURN_NOT_OK(MakeArrayData(child_type, -1, in_bufs, in_bufs_len,
                                    &child_array_data, buf_idx_ptr));
        auto child_array = arrow::MakeArray(child_array_data);
        auto list_array =
            arrow::ListArray::FromArrays(*offset_array, *child_array).ValueOrDie();
        *arr_data = list_array->data();

      } break;
      default:
        return arrow::Status::NotImplemented("MakeArrayData for type ", type->ToString(),
                                             " is not supported yet.");
    }

  } else {
    int64_t null_count = arrow::kUnknownNullCount;
    std::vector<std::shared_ptr<arrow::Buffer>> buffers;
    if (*buf_idx_ptr >= in_bufs_len) {
      return arrow::Status::Invalid("insufficient number of in_buf_addrs");
    }
    if (in_bufs[*buf_idx_ptr]->size() == 0) {
      null_count = 0;
    }
    buffers.push_back(in_bufs[*buf_idx_ptr]);
    *buf_idx_ptr += 1;

    if (arrow::is_binary_like(type->id())) {
      if (*buf_idx_ptr >= in_bufs_len) {
        return arrow::Status::Invalid("insufficient number of in_buf_addrs");
      }

      buffers.push_back(in_bufs[*buf_idx_ptr]);
      auto offsets_size = in_bufs[*buf_idx_ptr]->size();
      *buf_idx_ptr += 1;
      if (num_rows == -1) num_rows = offsets_size / 4 - 1;
    }

    if (*buf_idx_ptr >= in_bufs_len) {
      return arrow::Status::Invalid("insufficient number of in_buf_addrs");
    }
    auto value_size = in_bufs[*buf_idx_ptr]->size();
    buffers.push_back(in_bufs[*buf_idx_ptr]);
    *buf_idx_ptr += 1;
    if (num_rows == -1) {
      num_rows = value_size * 8 / arrow::bit_width(type->id());
    }

    *arr_data = arrow::ArrayData::Make(type, num_rows, std::move(buffers), null_count);
  }
  return arrow::Status::OK();
}

arrow::Status MakeRecordBatch(const std::shared_ptr<arrow::Schema>& schema, int num_rows,
                              std::vector<std::shared_ptr<arrow::Buffer>> in_bufs,
                              int in_bufs_len,
                              std::shared_ptr<arrow::RecordBatch>* batch) {
  std::vector<std::shared_ptr<arrow::ArrayData>> arrays;
  auto num_fields = schema->num_fields();
  int buf_idx = 0;

  for (int i = 0; i < num_fields; i++) {
    auto field = schema->field(i);
    std::shared_ptr<arrow::ArrayData> array_data;
    RETURN_NOT_OK(MakeArrayData(field->type(), num_rows, in_bufs, in_bufs_len,
                                &array_data, &buf_idx));
    arrays.push_back(array_data);
  }

  *batch = arrow::RecordBatch::Make(schema, num_rows, arrays);
  return arrow::Status::OK();
}

arrow::Status MakeRecordBatch(const std::shared_ptr<arrow::Schema>& schema, int num_rows,
                              int64_t* in_buf_addrs, int64_t* in_buf_sizes,
                              int in_bufs_len,
                              std::shared_ptr<arrow::RecordBatch>* batch) {
  std::vector<std::shared_ptr<arrow::ArrayData>> arrays;
  std::vector<std::shared_ptr<arrow::Buffer>> buffers;
  for (int i = 0; i < in_bufs_len; i++) {
    if (in_buf_addrs[i] != 0) {
      auto data = std::shared_ptr<arrow::Buffer>(new arrow::Buffer(
          reinterpret_cast<uint8_t*>(in_buf_addrs[i]), in_buf_sizes[i]));
      buffers.push_back(data);
    } else {
      buffers.push_back(std::make_shared<arrow::Buffer>(nullptr, 0));
    }
  }
  return MakeRecordBatch(schema, num_rows, buffers, in_bufs_len, batch);
}

std::string JStringToCString(JNIEnv* env, jstring string) {
  int32_t jlen, clen;
  clen = env->GetStringUTFLength(string);
  jlen = env->GetStringLength(string);
  std::vector<char> buffer(clen);
  env->GetStringUTFRegion(string, 0, jlen, buffer.data());
  return std::string(buffer.data(), clen);
}

arrow::Status MakeSchema(JNIEnv* env, jbyteArray schema_arr,
                         std::shared_ptr<arrow::Schema>* schema) {
  jsize schema_len = env->GetArrayLength(schema_arr);
  jbyte* schema_bytes = env->GetByteArrayElements(schema_arr, 0);

  auto serialized_schema =
      std::make_shared<arrow::Buffer>((uint8_t*)schema_bytes, schema_len);
  arrow::ipc::DictionaryMemo in_memo;
  arrow::io::BufferReader buf_reader(serialized_schema);
  *schema = arrow::ipc::ReadSchema(&buf_reader, &in_memo).ValueOrDie();
  env->ReleaseByteArrayElements(schema_arr, schema_bytes, JNI_ABORT);

  return arrow::Status::OK();
}

arrow::Status MakeExprVector(JNIEnv* env, jbyteArray exprs_arr,
                             gandiva::ExpressionVector* expr_vector,
                             gandiva::FieldVector* ret_types) {
  exprs::ExpressionList exprs;
  jsize exprs_len = env->GetArrayLength(exprs_arr);
  jbyte* exprs_bytes = env->GetByteArrayElements(exprs_arr, 0);

  if (!ParseProtobuf(reinterpret_cast<uint8_t*>(exprs_bytes), exprs_len, &exprs)) {
    env->ReleaseByteArrayElements(exprs_arr, exprs_bytes, JNI_ABORT);
    return arrow::Status::UnknownError("Unable to parse");
  }

  // create Expression out of the list of exprs
  for (int i = 0; i < exprs.exprs_size(); i++) {
    gandiva::ExpressionPtr root = ProtoTypeToExpression(exprs.exprs(i));

    if (root == nullptr) {
      env->ReleaseByteArrayElements(exprs_arr, exprs_bytes, JNI_ABORT);
      return arrow::Status::UnknownError("Unable to construct expression object");
    }

    expr_vector->push_back(root);
    ret_types->push_back(root->result());
  }

  return arrow::Status::OK();
}

jbyteArray ToSchemaByteArray(JNIEnv* env, std::shared_ptr<arrow::Schema> schema) {
  arrow::Status status;
  // std::shared_ptr<arrow::Buffer> buffer;
  arrow::Result<std::shared_ptr<arrow::Buffer>> maybe_buffer;
  maybe_buffer = arrow::ipc::SerializeSchema(*schema.get(), arrow::default_memory_pool());
  if (!status.ok()) {
    std::string error_message =
        "Unable to convert schema to byte array, err is " + status.message();
    throw gluten::GlutenException(error_message);
  }
  auto buffer = *std::move(maybe_buffer);
  jbyteArray out = env->NewByteArray(buffer->size());
  auto src = reinterpret_cast<const jbyte*>(buffer->data());
  env->SetByteArrayRegion(out, 0, buffer->size(), src);
  return out;
}

arrow::Result<arrow::Compression::type> GetCompressionType(JNIEnv* env,
                                                           jstring codec_jstr) {
  auto codec_l = env->GetStringUTFChars(codec_jstr, JNI_FALSE);

  std::string codec_u;
  std::transform(codec_l, codec_l + std::strlen(codec_l), std::back_inserter(codec_u),
                 ::tolower);

  ARROW_ASSIGN_OR_RAISE(auto compression_type,
                        arrow::util::Codec::GetCompressionType(codec_u));

  if (compression_type == arrow::Compression::LZ4) {
    compression_type = arrow::Compression::LZ4_FRAME;
  }
  env->ReleaseStringUTFChars(codec_jstr, codec_l);
  return compression_type;
}

Status DecompressBuffer(const arrow::Buffer& buffer, arrow::util::Codec* codec,
                        std::shared_ptr<arrow::Buffer>* out, arrow::MemoryPool* pool) {
  const uint8_t* data = buffer.data();
  int64_t compressed_size = buffer.size() - sizeof(int64_t);
  int64_t uncompressed_size =
      arrow::bit_util::FromLittleEndian(arrow::util::SafeLoadAs<int64_t>(data));
  ARROW_ASSIGN_OR_RAISE(auto uncompressed, AllocateBuffer(uncompressed_size, pool));

  int64_t actual_decompressed;
  ARROW_ASSIGN_OR_RAISE(
      actual_decompressed,
      codec->Decompress(compressed_size, data + sizeof(int64_t), uncompressed_size,
                        uncompressed->mutable_data()));
  if (actual_decompressed != uncompressed_size) {
    return Status::Invalid("Failed to fully decompress buffer, expected ",
                           uncompressed_size, " bytes but decompressed ",
                           actual_decompressed);
  }
  *out = std::move(uncompressed);
  return Status::OK();
}

Status DecompressBuffersByType(
    arrow::Compression::type compression, const arrow::ipc::IpcReadOptions& options,
    const uint8_t* buf_mask, std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    const std::vector<std::shared_ptr<arrow::Field>>& schema_fields) {
  std::unique_ptr<arrow::util::Codec> codec;
  std::unique_ptr<arrow::util::Codec> int32_codec;
  std::unique_ptr<arrow::util::Codec> int64_codec;
  ARROW_ASSIGN_OR_RAISE(codec, arrow::util::Codec::Create(arrow::Compression::LZ4_FRAME));
  ARROW_ASSIGN_OR_RAISE(int32_codec, arrow::util::Codec::CreateInt32(compression));
  ARROW_ASSIGN_OR_RAISE(int64_codec, arrow::util::Codec::CreateInt64(compression));

  int32_t buffer_idx = 0;
  for (const auto& field : schema_fields) {
    if (field->type()->id() == arrow::Type::NA) continue;

    const auto& layout_buffers = field->type()->layout().buffers;
    for (size_t i = 0; i < layout_buffers.size(); ++i) {
      const auto& layout = layout_buffers[i];
      auto& buffer = buffers[buffer_idx + i];
      if (buffer == nullptr || buffer->size() == 0) {
        continue;
      }
      // if the buffer has been rebuilt to uncompressed on java side, return
      if (arrow::bit_util::GetBit(buf_mask, buffer_idx + i)) {
        continue;
      }
      if (buffer->size() < 8) {
        return Status::Invalid(
            "Likely corrupted message, compressed buffers "
            "are larger than 8 bytes by construction");
      }
      switch (layout.kind) {
        case arrow::DataTypeLayout::BufferKind::FIXED_WIDTH:
          if (layout.byte_width == 4 && field->type()->id() != arrow::Type::FLOAT) {
            RETURN_NOT_OK(DecompressBuffer(*buffer, int32_codec.get(), &buffer,
                                           options.memory_pool));
          } else if (layout.byte_width == 8 &&
                     field->type()->id() != arrow::Type::DOUBLE) {
            RETURN_NOT_OK(DecompressBuffer(*buffer, int64_codec.get(), &buffer,
                                           options.memory_pool));
          } else {
            RETURN_NOT_OK(
                DecompressBuffer(*buffer, codec.get(), &buffer, options.memory_pool));
          }
          break;
        case arrow::DataTypeLayout::BufferKind::BITMAP:
        case arrow::DataTypeLayout::BufferKind::VARIABLE_WIDTH: {
          RETURN_NOT_OK(
              DecompressBuffer(*buffer, codec.get(), &buffer, options.memory_pool));
          break;
        }
        case arrow::DataTypeLayout::BufferKind::ALWAYS_NULL:
          break;
        default:
          return Status::Invalid("Wrong buffer layout.");
      }
    }
    buffer_idx += layout_buffers.size();
  }
  return arrow::Status::OK();
}

arrow::Status DecompressBuffers(
    arrow::Compression::type compression, const arrow::ipc::IpcReadOptions& options,
    const uint8_t* buf_mask, std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    const std::vector<std::shared_ptr<arrow::Field>>& schema_fields) {
  if (compression == arrow::Compression::FASTPFOR) {
    RETURN_NOT_OK(
        DecompressBuffersByType(compression, options, buf_mask, buffers, schema_fields));
    return arrow::Status::OK();
  }

  std::unique_ptr<arrow::util::Codec> codec;
  ARROW_ASSIGN_OR_RAISE(codec, arrow::util::Codec::Create(compression));

  auto DecompressOne = [&buffers, &buf_mask, &codec, &options](int i) {
    if (buffers[i] == nullptr || buffers[i]->size() == 0) {
      return arrow::Status::OK();
    }
    // if the buffer has been rebuilt to uncompressed on java side, return
    if (arrow::bit_util::GetBit(buf_mask, i)) {
      ARROW_ASSIGN_OR_RAISE(auto valid_copy,
                            buffers[i]->CopySlice(0, buffers[i]->size()));
      buffers[i] = valid_copy;
      return arrow::Status::OK();
    }

    if (buffers[i]->size() < 8) {
      return arrow::Status::Invalid(
          "Likely corrupted message, compressed buffers "
          "are larger than 8 bytes by construction");
    }
    RETURN_NOT_OK(
        DecompressBuffer(*buffers[i], codec.get(), &buffers[i], options.memory_pool));
    return arrow::Status::OK();
  };

  return ::arrow::internal::OptionalParallelFor(
      options.use_threads, static_cast<int>(buffers.size()), DecompressOne);
}
