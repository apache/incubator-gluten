
#include "VeloxBridge.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <string>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/c/helpers.h"
#include "arrow/extension_type.h"
#include "arrow/memory_pool.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/stl_allocator.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/util/small_vector.h"
#include "arrow/util/value_parsing.h"
#include "arrow/visit_type_inline.h"

namespace gluten {

namespace {

class ExportedArrayStreamByArray {
 public:
  struct PrivateData {
    explicit PrivateData(std::shared_ptr<gluten::ArrowArrayIterator> reader, std::shared_ptr<arrow::Schema> schema)
        : reader_(std::move(reader)), schema_(schema) {}

    std::shared_ptr<gluten::ArrowArrayIterator> reader_;
    std::string last_error_;
    std::shared_ptr<arrow::Schema> schema_;

    PrivateData() = default;
    ARROW_DISALLOW_COPY_AND_ASSIGN(PrivateData);
  };

  explicit ExportedArrayStreamByArray(struct ArrowArrayStream* stream) : stream_(stream) {}

  arrow::Status getSchema(struct ArrowSchema* outSchema) {
    return ExportSchema(*schema(), outSchema);
  }

  arrow::Status getNext(struct ArrowArray* outArray) {
    std::shared_ptr<ArrowArray> array;
    RETURN_NOT_OK(reader()->Next().Value(&array));
    if (array == nullptr) {
      // End of stream
      ArrowArrayMarkReleased(outArray);
    } else {
      ArrowArrayMove(array.get(), outArray);
    }
    return arrow::Status::OK();
  }

  const char* getLastError() {
    const auto& lastError = privateData()->last_error_;
    return lastError.empty() ? nullptr : lastError.c_str();
  }

  void release() {
    if (ArrowArrayStreamIsReleased(stream_)) {
      return;
    }
    DCHECK_NE(privateData(), nullptr);
    delete privateData();

    ArrowArrayStreamMarkReleased(stream_);
  }

  // C-compatible callbacks

  static int staticGetSchema(struct ArrowArrayStream* stream, struct ArrowSchema* outSchema) {
    ExportedArrayStreamByArray self{stream};
    return self.toCError(self.getSchema(outSchema));
  }

  static int staticGetNext(struct ArrowArrayStream* stream, struct ArrowArray* outArray) {
    ExportedArrayStreamByArray self{stream};
    return self.toCError(self.getNext(outArray));
  }

  static void staticRelease(struct ArrowArrayStream* stream) {
    ExportedArrayStreamByArray{stream}.release();
  }

  static const char* staticGetLastError(struct ArrowArrayStream* stream) {
    return ExportedArrayStreamByArray{stream}.getLastError();
  }

 private:
  int toCError(const arrow::Status& status) {
    if (ARROW_PREDICT_TRUE(status.ok())) {
      privateData()->last_error_.clear();
      return 0;
    }
    privateData()->last_error_ = status.ToString();
    switch (status.code()) {
      case arrow::StatusCode::IOError:
        return EIO;
      case arrow::StatusCode::NotImplemented:
        return ENOSYS;
      case arrow::StatusCode::OutOfMemory:
        return ENOMEM;
      default:
        return EINVAL; // Fallback for Invalid, TypeError, etc.
    }
  }

  PrivateData* privateData() {
    return reinterpret_cast<PrivateData*>(stream_->private_data);
  }

  const std::shared_ptr<gluten::ArrowArrayIterator> reader() {
    return privateData()->reader_;
  }

  const std::shared_ptr<arrow::Schema> schema() {
    return privateData()->schema_;
  }

  struct ArrowArrayStream* stream_;
};

} // namespace

arrow::Status exportArrowArray(
    std::shared_ptr<arrow::Schema> schema,
    std::shared_ptr<gluten::ArrowArrayIterator> reader,
    struct ArrowArrayStream* out) {
  out->get_schema = ExportedArrayStreamByArray::staticGetSchema;
  out->get_next = ExportedArrayStreamByArray::staticGetNext;
  out->get_last_error = ExportedArrayStreamByArray::staticGetLastError;
  out->release = ExportedArrayStreamByArray::staticRelease;
  out->private_data = new ExportedArrayStreamByArray::PrivateData{std::move(reader), std::move(schema)};
  return arrow::Status::OK();
}

//////////////////////////////////////////////////////////////////////////
// C stream import

// namespace {

// class ArrayStreamArrayReader : public ArrowArray {
//  public:
//   explicit ArrayStreamArrayReader(struct ArrowArrayStream* stream) {
//     ArrowArrayStreamMove(stream, &stream_);
//     DCHECK(!ArrowArrayStreamIsReleased(&stream_));
//   }

//   ~ArrayStreamArrayReader() {
//     ArrowArrayStreamRelease(&stream_);
//     DCHECK(ArrowArrayStreamIsReleased(&stream_));
//   }

//   std::shared_ptr<Schema> schema() const override { return CacheSchema(); }

//   Status ReadNext(std::shared_ptr<RecordBatch>* batch) override {
//     struct ArrowArray c_array;
//     RETURN_NOT_OK(StatusFromCError(stream_.get_next(&stream_, &c_array)));
//     if (ArrowArrayIsReleased(&c_array)) {
//       // End of stream
//       batch->reset();
//       return Status::OK();
//     } else {
//       return ImportRecordBatch(&c_array, CacheSchema()).Value(batch);
//     }
//   }

//  private:
//   std::shared_ptr<Schema> CacheSchema() const {
//     if (!schema_) {
//       struct ArrowSchema c_schema;
//       ARROW_CHECK_OK(StatusFromCError(stream_.get_schema(&stream_,
//       &c_schema))); schema_ = ImportSchema(&c_schema).ValueOrDie();
//     }
//     return schema_;
//   }

//   Status StatusFromCError(int errno_like) const {
//     if (ARROW_PREDICT_TRUE(errno_like == 0)) {
//       return Status::OK();
//     }
//     StatusCode code;
//     switch (errno_like) {
//       case EDOM:
//       case EINVAL:
//       case ERANGE:
//         code = StatusCode::Invalid;
//         break;
//       case ENOMEM:
//         code = StatusCode::OutOfMemory;
//         break;
//       case ENOSYS:
//         code = StatusCode::NotImplemented;
//       default:
//         code = StatusCode::IOError;
//         break;
//     }
//     const char* last_error = stream_.get_last_error(&stream_);
//     return Status(code, last_error ? std::string(last_error) : "");
//   }

//   mutable struct ArrowArrayStream stream_;
//   mutable std::shared_ptr<Schema> schema_;
// };

// }  // namespace

// Result<std::shared_ptr<ArrowArray>> ImportArrowArray(
//     struct ArrowArrayStream* stream) {
//   if (ArrowArrayStreamIsReleased(stream)) {
//     return Status::Invalid("Cannot import released ArrowArrayStream");
//   }
//   // XXX should we call get_schema() here to avoid crashing on error?
//   return std::make_shared<ArrayStreamArrayReader>(stream);
// }

} // namespace gluten
