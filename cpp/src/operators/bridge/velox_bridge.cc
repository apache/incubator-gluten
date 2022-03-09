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

#include "velox_bridge.h"

#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/stl_allocator.h>
#include <arrow/type_traits.h>
#include <arrow/util/bit_util.h>
#include <arrow/util/checked_cast.h>
#include <arrow/util/key_value_metadata.h>
#include <arrow/util/logging.h>
#include <arrow/util/macros.h>
#include <arrow/util/string_view.h>
#include <arrow/util/value_parsing.h>
#include <arrow/visitor_inline.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "velox_helpers.h"
#include "velox_util_internal.h"

namespace gazellejni {
namespace bridge {

using arrow::internal::checked_cast;
using arrow::internal::checked_pointer_cast;

// TODO export / import Extension types and arrays

namespace {

arrow::Status ExportingNotImplemented(const arrow::DataType& type) {
  return arrow::Status::NotImplemented("Exporting ", type.ToString(),
                                       " array not supported");
}

// Allocate exported private data using MemoryPool,
// to allow accounting memory and checking for memory leaks.

// XXX use Gandiva's SimpleArena?

template <typename T>
using PoolVector = std::vector<T, ::arrow::stl::allocator<T>>;

template <typename Derived>
struct PoolAllocationMixin {
  static void* operator new(size_t size) {
    DCHECK_EQ(size, sizeof(Derived));
    uint8_t* data;
    ARROW_CHECK_OK(
        arrow::default_memory_pool()->Allocate(static_cast<int64_t>(size), &data));
    return data;
  }

  static void operator delete(void* ptr) {
    arrow::default_memory_pool()->Free(reinterpret_cast<uint8_t*>(ptr), sizeof(Derived));
  }
};

//////////////////////////////////////////////////////////////////////////
// C schema export

struct ExportedSchemaPrivateData : PoolAllocationMixin<ExportedSchemaPrivateData> {
  std::string format_;
  std::string name_;
  std::string metadata_;
  struct ArrowSchema dictionary_;
  PoolVector<struct ArrowSchema> children_;
  PoolVector<struct ArrowSchema*> child_pointers_;

  ExportedSchemaPrivateData() = default;
  ARROW_DEFAULT_MOVE_AND_ASSIGN(ExportedSchemaPrivateData);
  ARROW_DISALLOW_COPY_AND_ASSIGN(ExportedSchemaPrivateData);
};

void ReleaseExportedSchema(struct ArrowSchema* schema) {
  if (VeloxArrowSchemaIsReleased(schema)) {
    return;
  }
  for (int64_t i = 0; i < schema->n_children; ++i) {
    struct ArrowSchema* child = schema->children[i];
    VeloxArrowSchemaRelease(child);
    DCHECK(VeloxArrowSchemaIsReleased(child))
        << "Child release callback should have marked it released";
  }
  struct ArrowSchema* dict = schema->dictionary;
  if (dict != nullptr) {
    VeloxArrowSchemaRelease(dict);
    DCHECK(VeloxArrowSchemaIsReleased(dict))
        << "Dictionary release callback should have marked it released";
  }
  DCHECK_NE(schema->private_data, nullptr);
  delete reinterpret_cast<ExportedSchemaPrivateData*>(schema->private_data);

  VeloxArrowSchemaMarkReleased(schema);
}

template <typename SizeType>
arrow::Result<int32_t> DowncastMetadataSize(SizeType size) {
  auto res = static_cast<int32_t>(size);
  if (res < 0 || static_cast<SizeType>(res) != size) {
    return arrow::Status::Invalid("Metadata too large (more than 2**31 items or bytes)");
  }
  return res;
}

arrow::Result<std::string> EncodeMetadata(const arrow::KeyValueMetadata& metadata) {
  ARROW_ASSIGN_OR_RAISE(auto npairs, DowncastMetadataSize(metadata.size()));
  std::string exported;

  // Pre-compute total string size
  size_t total_size = 4;
  for (int32_t i = 0; i < npairs; ++i) {
    total_size += 8 + metadata.key(i).length() + metadata.value(i).length();
  }
  exported.resize(total_size);

  char* data_start = &exported[0];
  char* data = data_start;
  auto write_int32 = [&](int32_t v) -> void {
    memcpy(data, &v, 4);
    data += 4;
  };
  auto write_string = [&](const std::string& s) -> arrow::Status {
    ARROW_ASSIGN_OR_RAISE(auto len, DowncastMetadataSize(s.length()));
    write_int32(len);
    if (len > 0) {
      memcpy(data, s.data(), len);
      data += len;
    }
    return arrow::Status::OK();
  };

  write_int32(npairs);
  for (int32_t i = 0; i < npairs; ++i) {
    RETURN_NOT_OK(write_string(metadata.key(i)));
    RETURN_NOT_OK(write_string(metadata.value(i)));
  }
  DCHECK_EQ(static_cast<size_t>(data - data_start), total_size);
  return exported;
}

struct SchemaExporter {
  arrow::Status ExportField(const arrow::Field& field) {
    export_.name_ = field.name();
    flags_ = field.nullable() ? ARROW_FLAG_NULLABLE : 0;

    const arrow::DataType& type = *field.type();
    RETURN_NOT_OK(ExportFormat(type));
    RETURN_NOT_OK(ExportChildren(type.fields()));
    RETURN_NOT_OK(ExportMetadata(field.metadata().get()));
    return arrow::Status::OK();
  }

  arrow::Status ExportType(const arrow::DataType& type) {
    flags_ = ARROW_FLAG_NULLABLE;

    RETURN_NOT_OK(ExportFormat(type));
    RETURN_NOT_OK(ExportChildren(type.fields()));
    return arrow::Status::OK();
  }

  arrow::Status ExportSchema(const arrow::Schema& schema) {
    static arrow::StructType dummy_struct_type({});
    flags_ = 0;

    RETURN_NOT_OK(ExportFormat(dummy_struct_type));
    RETURN_NOT_OK(ExportChildren(schema.fields()));
    RETURN_NOT_OK(ExportMetadata(schema.metadata().get()));
    return arrow::Status::OK();
  }

  // Finalize exporting by setting C struct fields and allocating
  // autonomous private data for each schema node.
  //
  // This function can't fail, as properly reclaiming memory in case of error
  // would be too fragile.  After this function returns, memory is reclaimed
  // by calling the release() pointer in the top level ArrowSchema struct.
  void Finish(struct ArrowSchema* c_struct) {
    // First, create permanent ExportedSchemaPrivateData
    auto pdata = new ExportedSchemaPrivateData(std::move(export_));

    // Second, finish dictionary and children.
    if (dict_exporter_) {
      dict_exporter_->Finish(&pdata->dictionary_);
    }
    pdata->child_pointers_.resize(child_exporters_.size(), nullptr);
    for (size_t i = 0; i < child_exporters_.size(); ++i) {
      auto ptr = pdata->child_pointers_[i] = &pdata->children_[i];
      child_exporters_[i].Finish(ptr);
    }

    // Third, fill C struct.
    DCHECK_NE(c_struct, nullptr);
    memset(c_struct, 0, sizeof(*c_struct));

    c_struct->format = pdata->format_.c_str();
    c_struct->name = pdata->name_.c_str();
    c_struct->metadata = pdata->metadata_.empty() ? nullptr : pdata->metadata_.c_str();
    c_struct->flags = flags_;

    c_struct->n_children = static_cast<int64_t>(child_exporters_.size());
    c_struct->children = pdata->child_pointers_.data();
    c_struct->dictionary = dict_exporter_ ? &pdata->dictionary_ : nullptr;
    c_struct->private_data = pdata;
    c_struct->release = ReleaseExportedSchema;
  }

  arrow::Status ExportFormat(const arrow::DataType& type) {
    if (type.id() == arrow::Type::DICTIONARY) {
      const auto& dict_type = checked_cast<const arrow::DictionaryType&>(type);
      if (dict_type.ordered()) {
        flags_ |= ARROW_FLAG_DICTIONARY_ORDERED;
      }
      // Dictionary type: parent struct describes index type,
      // child dictionary struct describes value type.
      RETURN_NOT_OK(arrow::VisitTypeInline(*dict_type.index_type(), this));
      dict_exporter_.reset(new SchemaExporter());
      RETURN_NOT_OK(dict_exporter_->ExportType(*dict_type.value_type()));
    } else {
      RETURN_NOT_OK(VisitTypeInline(type, this));
    }
    DCHECK(!export_.format_.empty());
    return arrow::Status::OK();
  }

  arrow::Status ExportChildren(const std::vector<std::shared_ptr<arrow::Field>>& fields) {
    export_.children_.resize(fields.size());
    child_exporters_.resize(fields.size());
    for (size_t i = 0; i < fields.size(); ++i) {
      RETURN_NOT_OK(child_exporters_[i].ExportField(*fields[i]));
    }
    return arrow::Status::OK();
  }

  arrow::Status ExportMetadata(const arrow::KeyValueMetadata* metadata) {
    if (metadata != nullptr && metadata->size() >= 0) {
      ARROW_ASSIGN_OR_RAISE(export_.metadata_, EncodeMetadata(*metadata));
    }
    return arrow::Status::OK();
  }

  arrow::Status SetFormat(std::string s) {
    export_.format_ = std::move(s);
    return arrow::Status::OK();
  }

  // Type-specific visitors

  arrow::Status Visit(const arrow::DataType& type) {
    return ExportingNotImplemented(type);
  }

  arrow::Status Visit(const arrow::NullType& type) { return SetFormat("n"); }

  arrow::Status Visit(const arrow::BooleanType& type) { return SetFormat("b"); }

  arrow::Status Visit(const arrow::Int8Type& type) { return SetFormat("c"); }

  arrow::Status Visit(const arrow::UInt8Type& type) { return SetFormat("C"); }

  arrow::Status Visit(const arrow::Int16Type& type) { return SetFormat("s"); }

  arrow::Status Visit(const arrow::UInt16Type& type) { return SetFormat("S"); }

  arrow::Status Visit(const arrow::Int32Type& type) { return SetFormat("i"); }

  arrow::Status Visit(const arrow::UInt32Type& type) { return SetFormat("I"); }

  arrow::Status Visit(const arrow::Int64Type& type) { return SetFormat("l"); }

  arrow::Status Visit(const arrow::UInt64Type& type) { return SetFormat("L"); }

  arrow::Status Visit(const arrow::HalfFloatType& type) { return SetFormat("e"); }

  arrow::Status Visit(const arrow::FloatType& type) { return SetFormat("f"); }

  arrow::Status Visit(const arrow::DoubleType& type) { return SetFormat("g"); }

  arrow::Status Visit(const arrow::FixedSizeBinaryType& type) {
    return SetFormat("w:" + std::to_string(type.byte_width()));
  }

  arrow::Status Visit(const arrow::DecimalType& type) {
    if (type.bit_width() == 128) {
      // 128 is the default bit-width
      return SetFormat("d:" + std::to_string(type.precision()) + "," +
                       std::to_string(type.scale()));
    } else {
      return SetFormat("d:" + std::to_string(type.precision()) + "," +
                       std::to_string(type.scale()) + "," +
                       std::to_string(type.bit_width()));
    }
  }

  arrow::Status Visit(const arrow::BinaryType& type) { return SetFormat("z"); }

  arrow::Status Visit(const arrow::LargeBinaryType& type) { return SetFormat("Z"); }

  arrow::Status Visit(const arrow::StringType& type) { return SetFormat("u"); }

  arrow::Status Visit(const arrow::LargeStringType& type) { return SetFormat("U"); }

  arrow::Status Visit(const arrow::Date32Type& type) { return SetFormat("tdD"); }

  arrow::Status Visit(const arrow::Date64Type& type) { return SetFormat("tdm"); }

  arrow::Status Visit(const arrow::Time32Type& type) {
    switch (type.unit()) {
      case arrow::TimeUnit::SECOND:
        export_.format_ = "tts";
        break;
      case arrow::TimeUnit::MILLI:
        export_.format_ = "ttm";
        break;
      default:
        return arrow::Status::Invalid("Invalid time unit for Time32: ", type.unit());
    }
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Time64Type& type) {
    switch (type.unit()) {
      case arrow::TimeUnit::MICRO:
        export_.format_ = "ttu";
        break;
      case arrow::TimeUnit::NANO:
        export_.format_ = "ttn";
        break;
      default:
        return arrow::Status::Invalid("Invalid time unit for Time64: ", type.unit());
    }
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::TimestampType& type) {
    switch (type.unit()) {
      case arrow::TimeUnit::SECOND:
        export_.format_ = "tss:";
        break;
      case arrow::TimeUnit::MILLI:
        export_.format_ = "tsm:";
        break;
      case arrow::TimeUnit::MICRO:
        export_.format_ = "tsu:";
        break;
      case arrow::TimeUnit::NANO:
        export_.format_ = "tsn:";
        break;
      default:
        return arrow::Status::Invalid("Invalid time unit for Timestamp: ", type.unit());
    }
    export_.format_ += type.timezone();
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::DurationType& type) {
    switch (type.unit()) {
      case arrow::TimeUnit::SECOND:
        export_.format_ = "tDs";
        break;
      case arrow::TimeUnit::MILLI:
        export_.format_ = "tDm";
        break;
      case arrow::TimeUnit::MICRO:
        export_.format_ = "tDu";
        break;
      case arrow::TimeUnit::NANO:
        export_.format_ = "tDn";
        break;
      default:
        return arrow::Status::Invalid("Invalid time unit for Duration: ", type.unit());
    }
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::MonthIntervalType& type) { return SetFormat("tiM"); }

  arrow::Status Visit(const arrow::DayTimeIntervalType& type) { return SetFormat("tiD"); }

  arrow::Status Visit(const arrow::ListType& type) { return SetFormat("+l"); }

  arrow::Status Visit(const arrow::LargeListType& type) { return SetFormat("+L"); }

  arrow::Status Visit(const arrow::FixedSizeListType& type) {
    return SetFormat("+w:" + std::to_string(type.list_size()));
  }

  arrow::Status Visit(const arrow::StructType& type) { return SetFormat("+s"); }

  arrow::Status Visit(const arrow::MapType& type) {
    export_.format_ = "+m";
    if (type.keys_sorted()) {
      flags_ |= ARROW_FLAG_MAP_KEYS_SORTED;
    }
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::UnionType& type) {
    std::string& s = export_.format_;
    s = "+u";
    if (type.mode() == arrow::UnionMode::DENSE) {
      s += "d:";
    } else {
      DCHECK_EQ(type.mode(), arrow::UnionMode::SPARSE);
      s += "s:";
    }
    bool first = true;
    for (const auto code : type.type_codes()) {
      if (!first) {
        s += ",";
      }
      s += std::to_string(code);
      first = false;
    }
    return arrow::Status::OK();
  }

  ExportedSchemaPrivateData export_;
  int64_t flags_ = 0;
  std::unique_ptr<SchemaExporter> dict_exporter_;
  std::vector<SchemaExporter> child_exporters_;
};

}  // namespace

arrow::Status ExportType(const arrow::DataType& type, struct ArrowSchema* out) {
  SchemaExporter exporter;
  RETURN_NOT_OK(exporter.ExportType(type));
  exporter.Finish(out);
  return arrow::Status::OK();
}

arrow::Status ExportField(const arrow::Field& field, struct ArrowSchema* out) {
  SchemaExporter exporter;
  RETURN_NOT_OK(exporter.ExportField(field));
  exporter.Finish(out);
  return arrow::Status::OK();
}

arrow::Status ExportSchema(const arrow::Schema& schema, struct ArrowSchema* out) {
  SchemaExporter exporter;
  RETURN_NOT_OK(exporter.ExportSchema(schema));
  exporter.Finish(out);
  return arrow::Status::OK();
}

//////////////////////////////////////////////////////////////////////////
// C data export

namespace {

struct ExportedArrayPrivateData : PoolAllocationMixin<ExportedArrayPrivateData> {
  // The buffers are owned by the ArrayData member
  PoolVector<const void*> buffers_;
  struct ArrowArray dictionary_;
  PoolVector<struct ArrowArray> children_;
  PoolVector<struct ArrowArray*> child_pointers_;

  std::shared_ptr<arrow::ArrayData> data_;

  ExportedArrayPrivateData() = default;
  ARROW_DEFAULT_MOVE_AND_ASSIGN(ExportedArrayPrivateData);
  ARROW_DISALLOW_COPY_AND_ASSIGN(ExportedArrayPrivateData);
};

void ReleaseExportedArray(struct ArrowArray* array) {
  if (VeloxArrowArrayIsReleased(array)) {
    return;
  }
  for (int64_t i = 0; i < array->n_children; ++i) {
    struct ArrowArray* child = array->children[i];
    VeloxArrowArrayRelease(child);
    DCHECK(VeloxArrowArrayIsReleased(child))
        << "Child release callback should have marked it released";
  }
  struct ArrowArray* dict = array->dictionary;
  if (dict != nullptr) {
    VeloxArrowArrayRelease(dict);
    DCHECK(VeloxArrowArrayIsReleased(dict))
        << "Dictionary release callback should have marked it released";
  }
  DCHECK_NE(array->private_data, nullptr);
  delete reinterpret_cast<ExportedArrayPrivateData*>(array->private_data);

  VeloxArrowArrayMarkReleased(array);
}

struct ArrayExporter {
  arrow::Status Export(const std::shared_ptr<arrow::ArrayData>& data) {
    // Force computing null count.
    // This is because ARROW-9037 is in version 0.17 and 0.17.1, and they are
    // not able to import arrays without a null bitmap and null_count == -1.
    data->GetNullCount();
    // Store buffer pointers
    export_.buffers_.resize(data->buffers.size());
    std::transform(data->buffers.begin(), data->buffers.end(), export_.buffers_.begin(),
                   [](const std::shared_ptr<arrow::Buffer>& buffer) -> const void* {
                     return buffer ? buffer->data() : nullptr;
                   });

    // Export dictionary
    if (data->dictionary != nullptr) {
      dict_exporter_.reset(new ArrayExporter());
      RETURN_NOT_OK(dict_exporter_->Export(data->dictionary));
    }

    // Export children
    export_.children_.resize(data->child_data.size());
    child_exporters_.resize(data->child_data.size());
    for (size_t i = 0; i < data->child_data.size(); ++i) {
      RETURN_NOT_OK(child_exporters_[i].Export(data->child_data[i]));
    }

    // Store owning pointer to ArrayData
    export_.data_ = data;

    return arrow::Status::OK();
  }

  // Finalize exporting by setting C struct fields and allocating
  // autonomous private data for each array node.
  //
  // This function can't fail, as properly reclaiming memory in case of error
  // would be too fragile.  After this function returns, memory is reclaimed
  // by calling the release() pointer in the top level ArrowArray struct.
  void Finish(struct ArrowArray* c_struct_) {
    // First, create permanent ExportedArrayPrivateData, to make sure that
    // child ArrayData pointers don't get invalidated.
    auto pdata = new ExportedArrayPrivateData(std::move(export_));
    const arrow::ArrayData& data = *pdata->data_;

    // Second, finish dictionary and children.
    if (dict_exporter_) {
      dict_exporter_->Finish(&pdata->dictionary_);
    }
    pdata->child_pointers_.resize(data.child_data.size(), nullptr);
    for (size_t i = 0; i < data.child_data.size(); ++i) {
      auto ptr = &pdata->children_[i];
      pdata->child_pointers_[i] = ptr;
      child_exporters_[i].Finish(ptr);
    }

    // Third, fill C struct.
    DCHECK_NE(c_struct_, nullptr);
    memset(c_struct_, 0, sizeof(*c_struct_));

    c_struct_->length = data.length;
    c_struct_->null_count = data.null_count;
    c_struct_->offset = data.offset;
    c_struct_->n_buffers = static_cast<int64_t>(pdata->buffers_.size());
    c_struct_->n_children = static_cast<int64_t>(pdata->child_pointers_.size());
    c_struct_->buffers = pdata->buffers_.data();
    c_struct_->children = pdata->child_pointers_.data();
    c_struct_->dictionary = dict_exporter_ ? &pdata->dictionary_ : nullptr;
    c_struct_->private_data = pdata;
    c_struct_->release = ReleaseExportedArray;
  }

  ExportedArrayPrivateData export_;
  std::unique_ptr<ArrayExporter> dict_exporter_;
  std::vector<ArrayExporter> child_exporters_;
};

}  // namespace

arrow::Status ExportArray(const arrow::Array& array, struct ArrowArray* out,
                          struct ArrowSchema* out_schema) {
  SchemaExportGuard guard(out_schema);
  if (out_schema != nullptr) {
    RETURN_NOT_OK(ExportType(*array.type(), out_schema));
  }
  ArrayExporter exporter;
  RETURN_NOT_OK(exporter.Export(array.data()));
  exporter.Finish(out);
  guard.Detach();
  return arrow::Status::OK();
}

arrow::Status ExportRecordBatch(const arrow::RecordBatch& batch, struct ArrowArray* out,
                                struct ArrowSchema* out_schema) {
  // XXX perhaps bypass ToStructArray() for speed?
  ARROW_ASSIGN_OR_RAISE(auto array, batch.ToStructArray());

  SchemaExportGuard guard(out_schema);
  if (out_schema != nullptr) {
    // Export the schema, not the struct type, so as not to lose top-level metadata
    RETURN_NOT_OK(ExportSchema(*batch.schema(), out_schema));
  }
  ArrayExporter exporter;
  RETURN_NOT_OK(exporter.Export(array->data()));
  exporter.Finish(out);
  guard.Detach();
  return arrow::Status::OK();
}

//////////////////////////////////////////////////////////////////////////
// C schema import

namespace {

static constexpr int64_t kMaxImportRecursionLevel = 64;

arrow::Status InvalidFormatString(arrow::util::string_view v) {
  return arrow::Status::Invalid("Invalid or unsupported format string: '", v, "'");
}

class FormatStringParser {
 public:
  FormatStringParser() {}

  explicit FormatStringParser(arrow::util::string_view v) : view_(v), index_(0) {}

  bool AtEnd() const { return index_ >= view_.length(); }

  char Next() { return view_[index_++]; }

  arrow::util::string_view Rest() { return view_.substr(index_); }

  arrow::Status CheckNext(char c) {
    if (AtEnd() || Next() != c) {
      return Invalid();
    }
    return arrow::Status::OK();
  }

  arrow::Status CheckHasNext() {
    if (AtEnd()) {
      return Invalid();
    }
    return arrow::Status::OK();
  }

  arrow::Status CheckAtEnd() {
    if (!AtEnd()) {
      return Invalid();
    }
    return arrow::Status::OK();
  }

  template <typename IntType = int32_t>
  arrow::Result<IntType> ParseInt(arrow::util::string_view v) {
    using ArrowIntType = typename arrow::CTypeTraits<IntType>::ArrowType;
    IntType value;
    if (!arrow::internal::ParseValue<ArrowIntType>(v.data(), v.size(), &value)) {
      return Invalid();
    }
    return value;
  }

  arrow::Result<arrow::TimeUnit::type> ParseTimeUnit() {
    RETURN_NOT_OK(CheckHasNext());
    switch (Next()) {
      case 's':
        return arrow::TimeUnit::SECOND;
      case 'm':
        return arrow::TimeUnit::MILLI;
      case 'u':
        return arrow::TimeUnit::MICRO;
      case 'n':
        return arrow::TimeUnit::NANO;
      default:
        return Invalid();
    }
  }

  std::vector<arrow::util::string_view> Split(arrow::util::string_view v,
                                              char delim = ',') {
    std::vector<arrow::util::string_view> parts;
    size_t start = 0, end;
    while (true) {
      end = v.find_first_of(delim, start);
      parts.push_back(v.substr(start, end - start));
      if (end == arrow::util::string_view::npos) {
        break;
      }
      start = end + 1;
    }
    return parts;
  }

  template <typename IntType = int32_t>
  arrow::Result<std::vector<IntType>> ParseInts(arrow::util::string_view v) {
    auto parts = Split(v);
    std::vector<IntType> result;
    result.reserve(parts.size());
    for (const auto& p : parts) {
      ARROW_ASSIGN_OR_RAISE(auto i, ParseInt<IntType>(p));
      result.push_back(i);
    }
    return result;
  }

  arrow::Status Invalid() { return InvalidFormatString(view_); }

 protected:
  arrow::util::string_view view_;
  size_t index_;
};

arrow::Result<std::shared_ptr<arrow::KeyValueMetadata>> DecodeMetadata(
    const char* metadata) {
  auto read_int32 = [&](int32_t* out) -> arrow::Status {
    int32_t v;
    memcpy(&v, metadata, 4);
    metadata += 4;
    *out = v;
    if (*out < 0) {
      return arrow::Status::Invalid("Invalid encoded metadata string");
    }
    return arrow::Status::OK();
  };

  auto read_string = [&](std::string* out) -> arrow::Status {
    int32_t len;
    RETURN_NOT_OK(read_int32(&len));
    out->resize(len);
    if (len > 0) {
      memcpy(&(*out)[0], metadata, len);
      metadata += len;
    }
    return arrow::Status::OK();
  };

  if (metadata == nullptr) {
    return nullptr;
  }
  int32_t npairs;
  RETURN_NOT_OK(read_int32(&npairs));
  if (npairs == 0) {
    return nullptr;
  }
  std::vector<std::string> keys(npairs);
  std::vector<std::string> values(npairs);
  for (int32_t i = 0; i < npairs; ++i) {
    RETURN_NOT_OK(read_string(&keys[i]));
    RETURN_NOT_OK(read_string(&values[i]));
  }
  return arrow::key_value_metadata(std::move(keys), std::move(values));
}

struct SchemaImporter {
  SchemaImporter() : c_struct_(nullptr), guard_(nullptr) {}

  arrow::Status Import(struct ArrowSchema* src) {
    if (VeloxArrowSchemaIsReleased(src)) {
      return arrow::Status::Invalid("Cannot import released ArrowSchema");
    }
    guard_.Reset(src);
    recursion_level_ = 0;
    c_struct_ = src;
    return DoImport();
  }

  arrow::Result<std::shared_ptr<arrow::Field>> MakeField() const {
    ARROW_ASSIGN_OR_RAISE(auto metadata, DecodeMetadata(c_struct_->metadata));
    const char* name = c_struct_->name ? c_struct_->name : "";
    bool nullable = (c_struct_->flags & ARROW_FLAG_NULLABLE) != 0;
    return arrow::field(name, type_, nullable, std::move(metadata));
  }

  arrow::Result<std::shared_ptr<arrow::Schema>> MakeSchema() const {
    if (type_->id() != arrow::Type::STRUCT) {
      return arrow::Status::Invalid(
          "Cannot import schema: ArrowSchema describes non-struct type ",
          type_->ToString());
    }
    ARROW_ASSIGN_OR_RAISE(auto metadata, DecodeMetadata(c_struct_->metadata));
    return arrow::schema(type_->fields(), std::move(metadata));
  }

  arrow::Result<std::shared_ptr<arrow::DataType>> MakeType() const { return type_; }

 protected:
  arrow::Status ImportChild(const SchemaImporter* parent, struct ArrowSchema* src) {
    if (VeloxArrowSchemaIsReleased(src)) {
      return arrow::Status::Invalid("Cannot import released ArrowSchema");
    }
    recursion_level_ = parent->recursion_level_ + 1;
    if (recursion_level_ >= kMaxImportRecursionLevel) {
      return arrow::Status::Invalid("Recursion level in ArrowSchema struct exceeded");
    }
    // The ArrowSchema is owned by its parent, so don't release it ourselves
    c_struct_ = src;
    return DoImport();
  }

  arrow::Status ImportDict(const SchemaImporter* parent, struct ArrowSchema* src) {
    return ImportChild(parent, src);
  }

  arrow::Status DoImport() {
    // First import children (required for reconstituting parent type)
    child_importers_.resize(c_struct_->n_children);
    for (int64_t i = 0; i < c_struct_->n_children; ++i) {
      DCHECK_NE(c_struct_->children[i], nullptr);
      RETURN_NOT_OK(child_importers_[i].ImportChild(this, c_struct_->children[i]));
    }

    // Import main type
    RETURN_NOT_OK(ProcessFormat());
    DCHECK_NE(type_, nullptr);

    // Import dictionary type
    if (c_struct_->dictionary != nullptr) {
      // Check this index type
      if (!is_integer(type_->id())) {
        return arrow::Status::Invalid(
            "ArrowSchema struct has a dictionary but is not an integer type: ",
            type_->ToString());
      }
      SchemaImporter dict_importer;
      RETURN_NOT_OK(dict_importer.ImportDict(this, c_struct_->dictionary));
      bool ordered = (c_struct_->flags & ARROW_FLAG_DICTIONARY_ORDERED) != 0;
      type_ = dictionary(type_, dict_importer.type_, ordered);
    }
    return arrow::Status::OK();
  }

  arrow::Status ProcessFormat() {
    f_parser_ = FormatStringParser(c_struct_->format);
    RETURN_NOT_OK(f_parser_.CheckHasNext());
    switch (f_parser_.Next()) {
      case 'n':
        return ProcessPrimitive(arrow::null());
      case 'b':
        return ProcessPrimitive(arrow::boolean());
      case 'c':
        return ProcessPrimitive(arrow::int8());
      case 'C':
        return ProcessPrimitive(arrow::uint8());
      case 's':
        return ProcessPrimitive(arrow::int16());
      case 'S':
        return ProcessPrimitive(arrow::uint16());
      case 'i':
        return ProcessPrimitive(arrow::int32());
      case 'I':
        return ProcessPrimitive(arrow::uint32());
      case 'l':
        return ProcessPrimitive(arrow::int64());
      case 'L':
        return ProcessPrimitive(arrow::uint64());
      case 'e':
        return ProcessPrimitive(arrow::float16());
      case 'f':
        return ProcessPrimitive(arrow::float32());
      case 'g':
        return ProcessPrimitive(arrow::float64());
      case 'u':
        return ProcessPrimitive(arrow::utf8());
      case 'U':
        return ProcessPrimitive(arrow::large_utf8());
      case 'z':
        return ProcessPrimitive(arrow::binary());
      case 'Z':
        return ProcessPrimitive(arrow::large_binary());
      case 'w':
        return ProcessFixedSizeBinary();
      case 'd':
        return ProcessDecimal();
      case 't':
        return ProcessTemporal();
      case '+':
        return ProcessNested();
    }
    return f_parser_.Invalid();
  }

  arrow::Status ProcessTemporal() {
    RETURN_NOT_OK(f_parser_.CheckHasNext());
    switch (f_parser_.Next()) {
      case 'd':
        return ProcessDate();
      case 't':
        return ProcessTime();
      case 'D':
        return ProcessDuration();
      case 'i':
        return ProcessInterval();
      case 's':
        return ProcessTimestamp();
    }
    return f_parser_.Invalid();
  }

  arrow::Status ProcessNested() {
    RETURN_NOT_OK(f_parser_.CheckHasNext());
    switch (f_parser_.Next()) {
      case 'l':
        return ProcessListLike<arrow::ListType>();
      case 'L':
        return ProcessListLike<arrow::LargeListType>();
      case 'w':
        return ProcessFixedSizeList();
      case 's':
        return ProcessStruct();
      case 'm':
        return ProcessMap();
      case 'u':
        return ProcessUnion();
    }
    return f_parser_.Invalid();
  }

  arrow::Status ProcessDate() {
    RETURN_NOT_OK(f_parser_.CheckHasNext());
    switch (f_parser_.Next()) {
      case 'D':
        return ProcessPrimitive(arrow::date32());
      case 'm':
        return ProcessPrimitive(arrow::date64());
    }
    return f_parser_.Invalid();
  }

  arrow::Status ProcessInterval() {
    RETURN_NOT_OK(f_parser_.CheckHasNext());
    switch (f_parser_.Next()) {
      case 'D':
        return ProcessPrimitive(arrow::day_time_interval());
      case 'M':
        return ProcessPrimitive(arrow::month_interval());
    }
    return f_parser_.Invalid();
  }

  arrow::Status ProcessTime() {
    ARROW_ASSIGN_OR_RAISE(auto unit, f_parser_.ParseTimeUnit());
    if (unit == arrow::TimeUnit::SECOND || unit == arrow::TimeUnit::MILLI) {
      return ProcessPrimitive(arrow::time32(unit));
    } else {
      return ProcessPrimitive(arrow::time64(unit));
    }
  }

  arrow::Status ProcessDuration() {
    ARROW_ASSIGN_OR_RAISE(auto unit, f_parser_.ParseTimeUnit());
    return ProcessPrimitive(arrow::duration(unit));
  }

  arrow::Status ProcessTimestamp() {
    ARROW_ASSIGN_OR_RAISE(auto unit, f_parser_.ParseTimeUnit());
    RETURN_NOT_OK(f_parser_.CheckNext(':'));
    type_ = timestamp(unit, std::string(f_parser_.Rest()));
    return arrow::Status::OK();
  }

  arrow::Status ProcessFixedSizeBinary() {
    RETURN_NOT_OK(f_parser_.CheckNext(':'));
    ARROW_ASSIGN_OR_RAISE(auto byte_width, f_parser_.ParseInt(f_parser_.Rest()));
    if (byte_width < 0) {
      return f_parser_.Invalid();
    }
    type_ = arrow::fixed_size_binary(byte_width);
    return arrow::Status::OK();
  }

  arrow::Status ProcessDecimal() {
    RETURN_NOT_OK(f_parser_.CheckNext(':'));
    ARROW_ASSIGN_OR_RAISE(auto prec_scale, f_parser_.ParseInts(f_parser_.Rest()));
    // 3 elements indicates bit width was communicated as well.
    if (prec_scale.size() != 2 && prec_scale.size() != 3) {
      return f_parser_.Invalid();
    }
    if (prec_scale[0] <= 0 || prec_scale[1] <= 0) {
      return f_parser_.Invalid();
    }
    if (prec_scale.size() == 2 || prec_scale[2] == 128) {
      type_ = arrow::decimal(prec_scale[0], prec_scale[1]);
    } else if (prec_scale[2] == 256) {
      type_ = arrow::decimal256(prec_scale[0], prec_scale[1]);
    } else {
      return f_parser_.Invalid();
    }
    return arrow::Status::OK();
  }

  arrow::Status ProcessPrimitive(const std::shared_ptr<arrow::DataType>& type) {
    RETURN_NOT_OK(f_parser_.CheckAtEnd());
    type_ = type;
    return CheckNoChildren(type);
  }

  template <typename ListType>
  arrow::Status ProcessListLike() {
    RETURN_NOT_OK(f_parser_.CheckAtEnd());
    RETURN_NOT_OK(CheckNumChildren(1));
    ARROW_ASSIGN_OR_RAISE(auto field, MakeChildField(0));
    type_ = std::make_shared<ListType>(field);
    return arrow::Status::OK();
  }

  arrow::Status ProcessMap() {
    RETURN_NOT_OK(f_parser_.CheckAtEnd());
    RETURN_NOT_OK(CheckNumChildren(1));
    ARROW_ASSIGN_OR_RAISE(auto field, MakeChildField(0));
    const auto& value_type = field->type();
    if (value_type->id() != arrow::Type::STRUCT) {
      return arrow::Status::Invalid(
          "Imported map array has unexpected child field type: ", field->ToString());
    }
    if (value_type->num_fields() != 2) {
      return arrow::Status::Invalid(
          "Imported map array has unexpected child field type: ", field->ToString());
    }

    bool keys_sorted = (c_struct_->flags & ARROW_FLAG_MAP_KEYS_SORTED);
    type_ = map(value_type->field(0)->type(), value_type->field(1)->type(), keys_sorted);
    return arrow::Status::OK();
  }

  arrow::Status ProcessFixedSizeList() {
    RETURN_NOT_OK(f_parser_.CheckNext(':'));
    ARROW_ASSIGN_OR_RAISE(auto list_size, f_parser_.ParseInt(f_parser_.Rest()));
    if (list_size < 0) {
      return f_parser_.Invalid();
    }
    RETURN_NOT_OK(CheckNumChildren(1));
    ARROW_ASSIGN_OR_RAISE(auto field, MakeChildField(0));
    type_ = fixed_size_list(field, list_size);
    return arrow::Status::OK();
  }

  arrow::Status ProcessStruct() {
    RETURN_NOT_OK(f_parser_.CheckAtEnd());
    ARROW_ASSIGN_OR_RAISE(auto fields, MakeChildFields());
    type_ = struct_(std::move(fields));
    return arrow::Status::OK();
  }

  arrow::Status ProcessUnion() {
    RETURN_NOT_OK(f_parser_.CheckHasNext());
    arrow::UnionMode::type mode;
    switch (f_parser_.Next()) {
      case 'd':
        mode = arrow::UnionMode::DENSE;
        break;
      case 's':
        mode = arrow::UnionMode::SPARSE;
        break;
      default:
        return f_parser_.Invalid();
    }
    RETURN_NOT_OK(f_parser_.CheckNext(':'));
    ARROW_ASSIGN_OR_RAISE(auto type_codes, f_parser_.ParseInts<int8_t>(f_parser_.Rest()));
    ARROW_ASSIGN_OR_RAISE(auto fields, MakeChildFields());
    if (fields.size() != type_codes.size()) {
      return arrow::Status::Invalid(
          "ArrowArray struct number of children incompatible with format string "
          "(mismatching number of union type codes) ",
          "'", c_struct_->format, "'");
    }
    for (const auto code : type_codes) {
      if (code < 0) {
        return arrow::Status::Invalid("Negative type code in union: format string '",
                                      c_struct_->format, "'");
      }
    }
    if (mode == arrow::UnionMode::SPARSE) {
      type_ = sparse_union(std::move(fields), std::move(type_codes));
    } else {
      type_ = dense_union(std::move(fields), std::move(type_codes));
    }
    return arrow::Status::OK();
  }

  arrow::Result<std::shared_ptr<arrow::Field>> MakeChildField(int64_t child_id) {
    const auto& child = child_importers_[child_id];
    if (child.c_struct_->name == nullptr) {
      return arrow::Status::Invalid("Expected non-null name in imported array child");
    }
    return child.MakeField();
  }

  arrow::Result<std::vector<std::shared_ptr<arrow::Field>>> MakeChildFields() {
    std::vector<std::shared_ptr<arrow::Field>> fields(child_importers_.size());
    for (int64_t i = 0; i < static_cast<int64_t>(child_importers_.size()); ++i) {
      ARROW_ASSIGN_OR_RAISE(fields[i], MakeChildField(i));
    }
    return fields;
  }

  arrow::Status CheckNoChildren(const std::shared_ptr<arrow::DataType>& type) {
    return CheckNumChildren(type, 0);
  }

  arrow::Status CheckNumChildren(const std::shared_ptr<arrow::DataType>& type,
                                 int64_t n_children) {
    if (c_struct_->n_children != n_children) {
      return arrow::Status::Invalid("Expected ", n_children,
                                    " children for imported type ", *type,
                                    ", ArrowArray struct has ", c_struct_->n_children);
    }
    return arrow::Status::OK();
  }

  arrow::Status CheckNumChildren(int64_t n_children) {
    if (c_struct_->n_children != n_children) {
      return arrow::Status::Invalid("Expected ", n_children,
                                    " children for imported format '", c_struct_->format,
                                    "', ArrowArray struct has ", c_struct_->n_children);
    }
    return arrow::Status::OK();
  }

  struct ArrowSchema* c_struct_;
  gazellejni::bridge::SchemaExportGuard guard_;
  FormatStringParser f_parser_;
  int64_t recursion_level_;
  std::vector<SchemaImporter> child_importers_;
  std::shared_ptr<arrow::DataType> type_;
};

}  // namespace

arrow::Result<std::shared_ptr<arrow::DataType>> ImportType(struct ArrowSchema* schema) {
  SchemaImporter importer;
  RETURN_NOT_OK(importer.Import(schema));
  return importer.MakeType();
}

arrow::Result<std::shared_ptr<arrow::Field>> ImportField(struct ArrowSchema* schema) {
  SchemaImporter importer;
  RETURN_NOT_OK(importer.Import(schema));
  return importer.MakeField();
}

arrow::Result<std::shared_ptr<arrow::Schema>> ImportSchema(struct ArrowSchema* schema) {
  SchemaImporter importer;
  RETURN_NOT_OK(importer.Import(schema));
  return importer.MakeSchema();
}

//////////////////////////////////////////////////////////////////////////
// C data import

namespace {

// A wrapper struct for an imported C ArrowArray.
// The ArrowArray is released on destruction.
struct ImportedArrayData {
  struct ArrowArray array_;

  ImportedArrayData() {
    VeloxArrowArrayMarkReleased(&array_);  // Initially released
  }

  void Release() {
    if (!VeloxArrowArrayIsReleased(&array_)) {
      VeloxArrowArrayRelease(&array_);
      DCHECK(VeloxArrowArrayIsReleased(&array_));
    }
  }

  ~ImportedArrayData() { Release(); }

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(ImportedArrayData);
};

// A buffer wrapping an imported piece of data.
class ImportedBuffer : public arrow::Buffer {
 public:
  ImportedBuffer(const uint8_t* data, int64_t size,
                 std::shared_ptr<ImportedArrayData> import)
      : arrow::Buffer(data, size), import_(std::move(import)) {}

  ~ImportedBuffer() override {}

 protected:
  std::shared_ptr<ImportedArrayData> import_;
};

struct ArrayImporter {
  explicit ArrayImporter(const std::shared_ptr<arrow::DataType>& type) : type_(type) {}

  arrow::Status Import(struct ArrowArray* src) {
    if (VeloxArrowArrayIsReleased(src)) {
      return arrow::Status::Invalid("Cannot import released ArrowArray");
    }
    recursion_level_ = 0;
    import_ = std::make_shared<ImportedArrayData>();
    c_struct_ = &import_->array_;
    VeloxArrowArrayMove(src, c_struct_);
    return DoImport();
  }

  arrow::Result<std::shared_ptr<arrow::Array>> MakeArray() {
    DCHECK_NE(data_, nullptr);
    return arrow::MakeArray(data_);
  }

  std::shared_ptr<arrow::ArrayData> GetArrayData() {
    DCHECK_NE(data_, nullptr);
    return data_;
  }

  arrow::Result<std::shared_ptr<arrow::RecordBatch>> MakeRecordBatch(
      std::shared_ptr<arrow::Schema> schema) {
    DCHECK_NE(data_, nullptr);
    if (data_->GetNullCount() != 0) {
      return arrow::Status::Invalid(
          "ArrowArray struct has non-zero null count, "
          "cannot be imported as RecordBatch");
    }
    if (data_->offset != 0) {
      return arrow::Status::Invalid(
          "ArrowArray struct has non-zero offset, "
          "cannot be imported as RecordBatch");
    }
    return arrow::RecordBatch::Make(std::move(schema), data_->length,
                                    std::move(data_->child_data));
  }

  arrow::Status ImportChild(const ArrayImporter* parent, struct ArrowArray* src) {
    if (VeloxArrowArrayIsReleased(src)) {
      return arrow::Status::Invalid("Cannot import released ArrowArray");
    }
    recursion_level_ = parent->recursion_level_ + 1;
    if (recursion_level_ >= kMaxImportRecursionLevel) {
      return arrow::Status::Invalid("Recursion level in ArrowArray struct exceeded");
    }
    // Child buffers will keep the entire parent import alive.
    // Perhaps we can move the child structs to an owned area
    // when the parent ImportedArrayData::Release() gets called,
    // but that is another level of complication.
    import_ = parent->import_;
    // The ArrowArray shouldn't be moved, it's owned by its parent
    c_struct_ = src;
    return DoImport();
  }

  arrow::Status ImportDict(const ArrayImporter* parent, struct ArrowArray* src) {
    return ImportChild(parent, src);
  }

  arrow::Status DoImport() {
    // First import children (required for reconstituting parent array data)
    const auto& fields = type_->fields();
    if (c_struct_->n_children != static_cast<int64_t>(fields.size())) {
      return arrow::Status::Invalid("ArrowArray struct has ", c_struct_->n_children,
                                    " children, expected ", fields.size(), " for type ",
                                    type_->ToString());
    }
    child_importers_.reserve(fields.size());
    for (int64_t i = 0; i < c_struct_->n_children; ++i) {
      DCHECK_NE(c_struct_->children[i], nullptr);
      child_importers_.emplace_back(fields[i]->type());
      RETURN_NOT_OK(child_importers_.back().ImportChild(this, c_struct_->children[i]));
    }

    // Import main data
    RETURN_NOT_OK(ImportMainData());

    bool is_dict_type = (type_->id() == arrow::Type::DICTIONARY);
    if (c_struct_->dictionary != nullptr) {
      if (!is_dict_type) {
        return arrow::Status::Invalid(
            "Import type is ", type_->ToString(),
            " but dictionary field in ArrowArray struct is not null");
      }
      const auto& dict_type = checked_cast<const arrow::DictionaryType&>(*type_);
      // Import dictionary values
      ArrayImporter dict_importer(dict_type.value_type());
      RETURN_NOT_OK(dict_importer.ImportDict(this, c_struct_->dictionary));
      data_->dictionary = dict_importer.GetArrayData();
    } else {
      if (is_dict_type) {
        return arrow::Status::Invalid(
            "Import type is ", type_->ToString(),
            " but dictionary field in ArrowArray struct is null");
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status ImportMainData() { return VisitTypeInline(*type_, this); }

  arrow::Status Visit(const arrow::DataType& type) {
    return arrow::Status::NotImplemented("Cannot import array of type ",
                                         type_->ToString());
  }

  arrow::Status Visit(const arrow::FixedWidthType& type) {
    return ImportFixedSizePrimitive();
  }

  arrow::Status Visit(const arrow::NullType& type) {
    RETURN_NOT_OK(CheckNoChildren());
    // XXX should we be lenient on the number of buffers?
    RETURN_NOT_OK(CheckNumBuffers(1));
    RETURN_NOT_OK(AllocateArrayData());
    RETURN_NOT_OK(ImportBitsBuffer(0));
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::StringType& type) { return ImportStringLike(type); }

  arrow::Status Visit(const arrow::BinaryType& type) { return ImportStringLike(type); }

  arrow::Status Visit(const arrow::LargeStringType& type) {
    return ImportStringLike(type);
  }

  arrow::Status Visit(const arrow::LargeBinaryType& type) {
    return ImportStringLike(type);
  }

  arrow::Status Visit(const arrow::ListType& type) { return ImportListLike(type); }

  arrow::Status Visit(const arrow::LargeListType& type) { return ImportListLike(type); }

  arrow::Status Visit(const arrow::FixedSizeListType& type) {
    RETURN_NOT_OK(CheckNumChildren(1));
    RETURN_NOT_OK(CheckNumBuffers(1));
    RETURN_NOT_OK(AllocateArrayData());
    RETURN_NOT_OK(ImportNullBitmap());
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::StructType& type) {
    RETURN_NOT_OK(CheckNumBuffers(1));
    RETURN_NOT_OK(AllocateArrayData());
    RETURN_NOT_OK(ImportNullBitmap());
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::UnionType& type) {
    auto mode = type.mode();
    if (mode == arrow::UnionMode::SPARSE) {
      RETURN_NOT_OK(CheckNumBuffers(2));
    } else {
      RETURN_NOT_OK(CheckNumBuffers(3));
    }
    RETURN_NOT_OK(AllocateArrayData());
    RETURN_NOT_OK(ImportNullBitmap());
    RETURN_NOT_OK(ImportFixedSizeBuffer(1, sizeof(int8_t)));
    if (mode == arrow::UnionMode::DENSE) {
      RETURN_NOT_OK(ImportFixedSizeBuffer(2, sizeof(int32_t)));
    }
    return arrow::Status::OK();
  }

  arrow::Status ImportFixedSizePrimitive() {
    const auto& fw_type = checked_cast<const arrow::FixedWidthType&>(*type_);
    RETURN_NOT_OK(CheckNoChildren());
    RETURN_NOT_OK(CheckNumBuffers(2));
    RETURN_NOT_OK(AllocateArrayData());
    RETURN_NOT_OK(ImportNullBitmap());
    if (arrow::BitUtil::IsMultipleOf8(fw_type.bit_width())) {
      RETURN_NOT_OK(ImportFixedSizeBuffer(1, fw_type.bit_width() / 8));
    } else {
      DCHECK_EQ(fw_type.bit_width(), 1);
      RETURN_NOT_OK(ImportBitsBuffer(1));
    }
    return arrow::Status::OK();
  }

  template <typename StringType>
  arrow::Status ImportStringLike(const StringType& type) {
    RETURN_NOT_OK(CheckNoChildren());
    RETURN_NOT_OK(CheckNumBuffers(3));
    RETURN_NOT_OK(AllocateArrayData());
    RETURN_NOT_OK(ImportNullBitmap());
    RETURN_NOT_OK(ImportOffsetsBuffer<typename StringType::offset_type>(1));
    RETURN_NOT_OK(ImportStringValuesBuffer<typename StringType::offset_type>(1, 2));
    return arrow::Status::OK();
  }

  template <typename ListType>
  arrow::Status ImportListLike(const ListType& type) {
    RETURN_NOT_OK(CheckNumChildren(1));
    RETURN_NOT_OK(CheckNumBuffers(2));
    RETURN_NOT_OK(AllocateArrayData());
    RETURN_NOT_OK(ImportNullBitmap());
    RETURN_NOT_OK(ImportOffsetsBuffer<typename ListType::offset_type>(1));
    return arrow::Status::OK();
  }

  arrow::Status CheckNoChildren() { return CheckNumChildren(0); }

  arrow::Status CheckNumChildren(int64_t n_children) {
    if (c_struct_->n_children != n_children) {
      return arrow::Status::Invalid("Expected ", n_children,
                                    " children for imported type ", type_->ToString(),
                                    ", ArrowArray struct has ", c_struct_->n_children);
    }
    return arrow::Status::OK();
  }

  arrow::Status CheckNumBuffers(int64_t n_buffers) {
    if (n_buffers != c_struct_->n_buffers) {
      return arrow::Status::Invalid("Expected ", n_buffers, " buffers for imported type ",
                                    type_->ToString(), ", ArrowArray struct has ",
                                    c_struct_->n_buffers);
    }
    return arrow::Status::OK();
  }

  arrow::Status AllocateArrayData() {
    DCHECK_EQ(data_, nullptr);
    data_ = std::make_shared<arrow::ArrayData>(type_, c_struct_->length,
                                               c_struct_->null_count, c_struct_->offset);
    data_->buffers.resize(static_cast<size_t>(c_struct_->n_buffers));
    data_->child_data.resize(static_cast<size_t>(c_struct_->n_children));
    DCHECK_EQ(child_importers_.size(), data_->child_data.size());
    std::transform(child_importers_.begin(), child_importers_.end(),
                   data_->child_data.begin(),
                   [](const ArrayImporter& child) { return child.data_; });
    return arrow::Status::OK();
  }

  arrow::Status ImportNullBitmap(int32_t buffer_id = 0) {
    RETURN_NOT_OK(ImportBitsBuffer(buffer_id));
    if (data_->null_count > 0 && data_->buffers[buffer_id] == nullptr) {
      return arrow::Status::Invalid(
          "ArrowArray struct has null bitmap buffer but non-zero null_count ",
          data_->null_count);
    }
    return arrow::Status::OK();
  }

  arrow::Status ImportBitsBuffer(int32_t buffer_id) {
    // Compute visible size of buffer
    int64_t buffer_size =
        arrow::BitUtil::BytesForBits(c_struct_->length + c_struct_->offset);
    return ImportBuffer(buffer_id, buffer_size);
  }

  arrow::Status ImportFixedSizeBuffer(int32_t buffer_id, int64_t byte_width) {
    // Compute visible size of buffer
    int64_t buffer_size = byte_width * (c_struct_->length + c_struct_->offset);
    return ImportBuffer(buffer_id, buffer_size);
  }

  template <typename OffsetType>
  arrow::Status ImportOffsetsBuffer(int32_t buffer_id) {
    // Compute visible size of buffer
    int64_t buffer_size =
        sizeof(OffsetType) * (c_struct_->length + c_struct_->offset + 1);
    return ImportBuffer(buffer_id, buffer_size);
  }

  template <typename OffsetType>
  arrow::Status ImportStringValuesBuffer(int32_t offsets_buffer_id, int32_t buffer_id,
                                         int64_t byte_width = 1) {
    auto offsets = data_->GetValues<OffsetType>(offsets_buffer_id);
    // Compute visible size of buffer
    int64_t buffer_size = byte_width * offsets[c_struct_->length];
    return ImportBuffer(buffer_id, buffer_size);
  }

  arrow::Status ImportBuffer(int32_t buffer_id, int64_t buffer_size) {
    std::shared_ptr<arrow::Buffer>* out = &data_->buffers[buffer_id];
    auto data = reinterpret_cast<const uint8_t*>(c_struct_->buffers[buffer_id]);
    if (data != nullptr) {
      *out = std::make_shared<ImportedBuffer>(data, buffer_size, import_);
    } else {
      out->reset();
    }
    return arrow::Status::OK();
  }

  struct ArrowArray* c_struct_;
  int64_t recursion_level_;
  const std::shared_ptr<arrow::DataType>& type_;

  std::shared_ptr<ImportedArrayData> import_;
  std::shared_ptr<arrow::ArrayData> data_;
  std::vector<ArrayImporter> child_importers_;
};

}  // namespace

arrow::Result<std::shared_ptr<arrow::Array>> ImportArray(
    struct ArrowArray* array, std::shared_ptr<arrow::DataType> type) {
  ArrayImporter importer(type);
  RETURN_NOT_OK(importer.Import(array));
  return importer.MakeArray();
}

arrow::Result<std::shared_ptr<arrow::Array>> ImportArray(struct ArrowArray* array,
                                                         struct ArrowSchema* type) {
  auto maybe_type = ImportType(type);
  if (!maybe_type.ok()) {
    VeloxArrowArrayRelease(array);
    return maybe_type.status();
  }
  return ImportArray(array, *maybe_type);
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> ImportRecordBatch(
    struct ArrowArray* array, std::shared_ptr<arrow::Schema> schema) {
  auto type = struct_(schema->fields());
  ArrayImporter importer(type);
  RETURN_NOT_OK(importer.Import(array));
  return importer.MakeRecordBatch(std::move(schema));
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> ImportRecordBatch(
    struct ArrowArray* array, struct ArrowSchema* schema) {
  auto maybe_schema = ImportSchema(schema);
  if (!maybe_schema.ok()) {
    VeloxArrowArrayRelease(array);
    return maybe_schema.status();
  }
  return ImportRecordBatch(array, *maybe_schema);
}

//////////////////////////////////////////////////////////////////////////
// C stream export

namespace {

class ExportedArrayStream {
 public:
  struct PrivateData {
    explicit PrivateData(std::shared_ptr<arrow::RecordBatchReader> reader)
        : reader_(std::move(reader)) {}

    std::shared_ptr<arrow::RecordBatchReader> reader_;
    std::string last_error_;

    PrivateData() = default;
    ARROW_DISALLOW_COPY_AND_ASSIGN(PrivateData);
  };

  explicit ExportedArrayStream(struct ArrowArrayStream* stream) : stream_(stream) {}

  arrow::Status GetSchema(struct ArrowSchema* out_schema) {
    return ExportSchema(*reader()->schema(), out_schema);
  }

  arrow::Status GetNext(struct ArrowArray* out_array) {
    std::shared_ptr<arrow::RecordBatch> batch;
    RETURN_NOT_OK(reader()->ReadNext(&batch));
    if (batch == nullptr) {
      // End of stream
      VeloxArrowArrayMarkReleased(out_array);
      return arrow::Status::OK();
    } else {
      return ExportRecordBatch(*batch, out_array);
    }
  }

  const char* GetLastError() {
    const auto& last_error = private_data()->last_error_;
    return last_error.empty() ? nullptr : last_error.c_str();
  }

  void Release() {
    if (VeloxArrowArrayStreamIsReleased(stream_)) {
      return;
    }
    DCHECK_NE(private_data(), nullptr);
    delete private_data();

    VeloxArrowArrayStreamMarkReleased(stream_);
  }

  // C-compatible callbacks

  static int StaticGetSchema(struct ArrowArrayStream* stream,
                             struct ArrowSchema* out_schema) {
    ExportedArrayStream self{stream};
    return self.ToCError(self.GetSchema(out_schema));
  }

  static int StaticGetNext(struct ArrowArrayStream* stream,
                           struct ArrowArray* out_array) {
    ExportedArrayStream self{stream};
    return self.ToCError(self.GetNext(out_array));
  }

  static void StaticRelease(struct ArrowArrayStream* stream) {
    ExportedArrayStream{stream}.Release();
  }

  static const char* StaticGetLastError(struct ArrowArrayStream* stream) {
    return ExportedArrayStream{stream}.GetLastError();
  }

 private:
  int ToCError(const arrow::Status& status) {
    if (ARROW_PREDICT_TRUE(status.ok())) {
      private_data()->last_error_.clear();
      return 0;
    }
    private_data()->last_error_ = status.ToString();
    switch (status.code()) {
      case arrow::StatusCode::IOError:
        return EIO;
      case arrow::StatusCode::NotImplemented:
        return ENOSYS;
      case arrow::StatusCode::OutOfMemory:
        return ENOMEM;
      default:
        return EINVAL;  // Fallback for Invalid, TypeError, etc.
    }
  }

  PrivateData* private_data() {
    return reinterpret_cast<PrivateData*>(stream_->private_data);
  }

  const std::shared_ptr<arrow::RecordBatchReader>& reader() {
    return private_data()->reader_;
  }

  struct ArrowArrayStream* stream_;
};

}  // namespace

arrow::Status ExportRecordBatchReader(std::shared_ptr<arrow::RecordBatchReader> reader,
                                      struct ArrowArrayStream* out) {
  out->get_schema = ExportedArrayStream::StaticGetSchema;
  out->get_next = ExportedArrayStream::StaticGetNext;
  out->get_last_error = ExportedArrayStream::StaticGetLastError;
  out->release = ExportedArrayStream::StaticRelease;
  out->private_data = new ExportedArrayStream::PrivateData{std::move(reader)};
  return arrow::Status::OK();
}

//////////////////////////////////////////////////////////////////////////
// C stream import

namespace {

class ArrayStreamBatchReader : public arrow::RecordBatchReader {
 public:
  explicit ArrayStreamBatchReader(struct ArrowArrayStream* stream) {
    VeloxArrowArrayStreamMove(stream, &stream_);
    DCHECK(!VeloxArrowArrayStreamIsReleased(&stream_));
  }

  ~ArrayStreamBatchReader() {
    VeloxArrowArrayStreamRelease(&stream_);
    DCHECK(VeloxArrowArrayStreamIsReleased(&stream_));
  }

  std::shared_ptr<arrow::Schema> schema() const override { return CacheSchema(); }

  arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* batch) override {
    struct ArrowArray c_array;
    RETURN_NOT_OK(StatusFromCError(stream_.get_next(&stream_, &c_array)));
    if (VeloxArrowArrayIsReleased(&c_array)) {
      // End of stream
      batch->reset();
      return arrow::Status::OK();
    } else {
      return ImportRecordBatch(&c_array, CacheSchema()).Value(batch);
    }
  }

 private:
  std::shared_ptr<arrow::Schema> CacheSchema() const {
    if (!schema_) {
      struct ArrowSchema c_schema;
      ARROW_CHECK_OK(StatusFromCError(stream_.get_schema(&stream_, &c_schema)));
      schema_ = ImportSchema(&c_schema).ValueOrDie();
    }
    return schema_;
  }

  arrow::Status StatusFromCError(int errno_like) const {
    if (ARROW_PREDICT_TRUE(errno_like == 0)) {
      return arrow::Status::OK();
    }
    arrow::StatusCode code;
    switch (errno_like) {
      case EDOM:
      case EINVAL:
      case ERANGE:
        code = arrow::StatusCode::Invalid;
        break;
      case ENOMEM:
        code = arrow::StatusCode::OutOfMemory;
        break;
      case ENOSYS:
        code = arrow::StatusCode::NotImplemented;
      default:
        code = arrow::StatusCode::IOError;
        break;
    }
    const char* last_error = stream_.get_last_error(&stream_);
    return arrow::Status(code, last_error ? std::string(last_error) : "");
  }

  mutable struct ArrowArrayStream stream_;
  mutable std::shared_ptr<arrow::Schema> schema_;
};

}  // namespace

arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> ImportRecordBatchReader(
    struct ArrowArrayStream* stream) {
  if (VeloxArrowArrayStreamIsReleased(stream)) {
    return arrow::Status::Invalid("Cannot import released ArrowArrayStream");
  }
  // XXX should we call get_schema() here to avoid crashing on error?
  return std::make_shared<ArrayStreamBatchReader>(stream);
}

}  // namespace bridge
}  // namespace gazellejni
