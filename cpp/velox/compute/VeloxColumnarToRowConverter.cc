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

#include "VeloxColumnarToRowConverter.h"

#include <arrow/array/array_base.h>
#include <arrow/buffer.h>
#include <arrow/c/abi.h>
#include <arrow/type_traits.h>
#include <arrow/util/decimal.h>

#include "ArrowTypeUtils.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "memory/VeloxColumnarBatch.h"
#include "velox/row/UnsafeRowDeserializers.h"
#include "velox/row/UnsafeRowFast.h"
#include "velox/vector/arrow/Bridge.h"

using namespace facebook;
using arrow::MemoryPool;

namespace gluten {

arrow::Status VeloxColumnarToRowConverter::init() {
  numRows_ = rv_->size();
  numCols_ = rv_->childrenSize();

  ArrowSchema cSchema{};
  velox::exportToArrow(rv_, cSchema);
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> schema, arrow::ImportSchema(&cSchema));
  if (numCols_ != schema->num_fields()) {
    return arrow::Status::Invalid("Mismatch: num_cols_ != schema->num_fields()");
  }
  schema_ = schema;

  // The input is Arrow batch. We need to resume Velox Vector here.
  vecs_.clear();
  resumeVeloxVector();

  // Calculate the initial size
  nullBitsetWidthInBytes_ = calculateBitSetWidthInBytes(numCols_);
  int64_t fixedSizePerRow = calculatedFixeSizePerRow(schema_, numCols_);

  // Initialize the offsets_ , lengths_, buffer_cursor_
  lengths_.clear();
  offsets_.clear();
  bufferCursor_.clear();

  lengths_.resize(numRows_, fixedSizePerRow);
  offsets_.resize(numRows_, 0);
  bufferCursor_.resize(numRows_, nullBitsetWidthInBytes_ + 8 * numCols_);

  // Calculated the lengths_
  for (int64_t colIdx = 0; colIdx < numCols_; colIdx++) {
    std::shared_ptr<arrow::Field> field = schema_->field(colIdx);
    if (arrow::is_binary_like(field->type()->id())) {
      auto strViews = vecs_[colIdx]->asFlatVector<velox::StringView>()->rawValues();
      for (int rowIdx = 0; rowIdx < numRows_; rowIdx++) {
        auto length = strViews[rowIdx].size();
        int64_t bytes = roundNumberOfBytesToNearestWord(length);
        lengths_[rowIdx] += bytes;
      }
    }
  }

  // Calculated the offsets_  and total memory size based on lengths_
  int64_t totalMemorySize = lengths_[0];
  for (int64_t rowIdx = 1; rowIdx < numRows_; rowIdx++) {
    offsets_[rowIdx] = offsets_[rowIdx - 1] + lengths_[rowIdx - 1];
    totalMemorySize += lengths_[rowIdx];
  }

  if (buffer_ == nullptr) {
    // First allocate memory
    ARROW_ASSIGN_OR_RAISE(buffer_, arrow::AllocateBuffer(totalMemorySize * 1.2, arrowPool_.get()));
  } else if (buffer_->capacity() < totalMemorySize) {
    ARROW_ASSIGN_OR_RAISE(buffer_, arrow::AllocateBuffer(totalMemorySize * 1.2, arrowPool_.get()));
  }

  bufferAddress_ = buffer_->mutable_data();
  memset(bufferAddress_, 0, sizeof(int8_t) * totalMemorySize);
  return arrow::Status::OK();
}

void VeloxColumnarToRowConverter::resumeVeloxVector() {
  vecs_.reserve(numCols_);
  for (int colIdx = 0; colIdx < numCols_; colIdx++) {
    vecs_.push_back(rv_->childAt(colIdx));
  }
}

arrow::Status VeloxColumnarToRowConverter::write(std::shared_ptr<ColumnarBatch> cb) {
  auto veloxBatch = std::dynamic_pointer_cast<VeloxColumnarBatch>(cb);
  rv_ = veloxBatch->getFlattenedRowVector();
  RETURN_NOT_OK(init());
  for (int col_idx = 0; col_idx < numCols_; col_idx++) {
    auto vec = vecs_[col_idx];
    bool mayHaveNulls = vec->mayHaveNulls();

    int64_t field_offset = getFieldOffset(nullBitsetWidthInBytes_, col_idx);
    velox::row::UnsafeRowFast fast(rv_);
#define SERIALIZE_COLUMN(DataType)                                             \
  do {                                                                         \
    if (mayHaveNulls) {                                                        \
      for (int row_idx = 0; row_idx < numRows_; row_idx++) {                   \
        if (vec->isNullAt(row_idx)) {                                          \
          setNullAt(bufferAddress_, offsets_[row_idx], field_offset, col_idx); \
        } else {                                                               \
          auto write_address = (char*)(bufferAddress_ + offsets_[row_idx]);    \
          fast.serialize(row_idx, write_address);                              \
        }                                                                      \
      }                                                                        \
    } else {                                                                   \
      for (int row_idx = 0; row_idx < numRows_; row_idx++) {                   \
        auto write_address = (char*)(bufferAddress_ + offsets_[row_idx]);      \
        fast.serialize(row_idx, write_address);                                \
      }                                                                        \
    }                                                                          \
  } while (0)

    auto colTypeId = schema_->field(col_idx)->type()->id();
    switch (colTypeId) {
      // We should keep supported types consistent with that in #buildCheck of GlutenColumnarToRowExec.scala.
      case arrow::Int8Type::type_id: {
        SERIALIZE_COLUMN(TinyintType);
        break;
      }
      case arrow::Int16Type::type_id: {
        SERIALIZE_COLUMN(SmallintType);
        break;
      }
      case arrow::Int32Type::type_id: {
        SERIALIZE_COLUMN(IntegerType);
        break;
      }
      case arrow::Int64Type::type_id: {
        SERIALIZE_COLUMN(BigintType);
        break;
      }
      case arrow::Date32Type::type_id: {
        SERIALIZE_COLUMN(DateType);
        break;
      }
      case arrow::FloatType::type_id: {
        SERIALIZE_COLUMN(RealType);
        break;
      }
      case arrow::DoubleType::type_id: {
        SERIALIZE_COLUMN(DoubleType);
        break;
      }
      case arrow::BooleanType::type_id: {
        SERIALIZE_COLUMN(BooleanType);
        break;
      }
      case arrow::BinaryType::type_id:
      case arrow::StringType::type_id: {
        auto strViews = vec->asFlatVector<velox::StringView>()->rawValues();
        for (int rowIdx = 0; rowIdx < numRows_; rowIdx++) {
          if (mayHaveNulls && vec->isNullAt(rowIdx)) {
            setNullAt(bufferAddress_, offsets_[rowIdx], field_offset, col_idx);
          } else {
            int32_t length = (int32_t)strViews[rowIdx].size();
            auto value = strViews[rowIdx].data();
            // Write the variable value.
            memcpy(bufferAddress_ + offsets_[rowIdx] + bufferCursor_[rowIdx], value, length);
            int64_t offsetAndSize = ((int64_t)bufferCursor_[rowIdx] << 32) | length;
            // Write the offset and size.
            memcpy(bufferAddress_ + offsets_[rowIdx] + field_offset, &offsetAndSize, sizeof(int64_t));
            bufferCursor_[rowIdx] += length;
          }
        }
        break;
      }
      case arrow::Decimal128Type::type_id: {
        for (auto rowIdx = 0; rowIdx < numRows_; rowIdx++) {
          bool flag = vec->isNullAt(rowIdx);
          if (vec->typeKind() == velox::TypeKind::BIGINT) {
            auto shortDecimal = vec->asFlatVector<int64_t>()->rawValues();
            if (!flag) {
              // Get the long value and write the long value
              // Refer to the int64_t() method of Decimal128
              int64_t longValue = shortDecimal[rowIdx];
              memcpy(bufferAddress_ + offsets_[rowIdx] + field_offset, &longValue, sizeof(long));
            } else {
              setNullAt(bufferAddress_, offsets_[rowIdx], field_offset, col_idx);
            }
          } else {
            if (flag) {
              setNullAt(bufferAddress_, offsets_[rowIdx], field_offset, col_idx);
            } else {
              auto longDecimal = vec->asFlatVector<facebook::velox::int128_t>()->rawValues();
              int32_t size;
              velox::int128_t veloxInt128 = longDecimal[rowIdx];

              velox::int128_t orignalValue = veloxInt128;
              int64_t high = veloxInt128 >> 64;
              uint64_t lower = (uint64_t)orignalValue;

              auto out = toByteArray(arrow::Decimal128(high, lower), &size);
              assert(size <= 16);

              // write the variable value
              memcpy(bufferAddress_ + bufferCursor_[rowIdx] + offsets_[rowIdx], &out[0], size);
              // write the offset and size
              int64_t offsetAndSize = ((int64_t)bufferCursor_[rowIdx] << 32) | size;
              memcpy(bufferAddress_ + offsets_[rowIdx] + field_offset, &offsetAndSize, sizeof(int64_t));
            }

            // Update the cursor of the buffer.
            bufferCursor_[rowIdx] += 16;
          }
        }
        break;
      }
      case arrow::TimestampType::type_id: {
        SERIALIZE_COLUMN(TimestampType);
        break;
      }
      default:
        return arrow::Status::Invalid(
            "Type " + schema_->field(col_idx)->type()->name() + " is not supported in VeloxToRow conversion.");
    }

#undef SERIALIZE_COLUMN
  }
  return arrow::Status::OK();
}

} // namespace gluten
