#pragma once

#include "arrow/util/iterator.h"
#include "compute/transfer_iterator.h"
#include "include/arrow/c/bridge.h"

namespace gluten {

using ArrowArrayIterator = arrow::Iterator<std::shared_ptr<ArrowArray>>;

class ArrowArrayStreamWrapper {
 public:
  struct PrivateData {
    PrivateData(std::unique_ptr<gluten::TransferIterator>&& iterator, std::shared_ptr<arrow::Schema> schema)
        : iterator_(std::move(iterator)), schema_(schema) {}

    std::unique_ptr<gluten::TransferIterator> iterator_;
    std::shared_ptr<arrow::Schema> schema_;
    std::string last_error_;

    PrivateData() = default;
    ARROW_DISALLOW_COPY_AND_ASSIGN(PrivateData);
  };

  explicit ArrowArrayStreamWrapper(struct ArrowArrayStream* stream) : stream_(stream) {}

  arrow::Status GetSchema(struct ArrowSchema* out_schema) {
    return ExportSchema(*schema(), out_schema);
  }

  arrow::Status GetNext(struct ArrowArray* out_array) {
    std::shared_ptr<ArrowArray> array = iterator()->Next();
    if (array == nullptr) {
      // End of stream
      ArrowArrayMarkReleased(out_array);
    } else {
      ArrowArrayMove(array.get(), out_array);
    }
    return arrow::Status::OK();
  }

  const char* GetLastError() const {
    const auto& last_error = private_data()->last_error_;
    return last_error.empty() ? nullptr : last_error.c_str();
  }

  void Release() {
    if (ArrowArrayStreamIsReleased(stream_)) {
      return;
    }
    delete private_data();

    ArrowArrayStreamMarkReleased(stream_);
  }

  // C-compatible callbacks

  static int StaticGetSchema(struct ArrowArrayStream* stream, struct ArrowSchema* out_schema) {
    ArrowArrayStreamWrapper self{stream};
    return self.ToCError(self.GetSchema(out_schema));
  }

  static int StaticGetNext(struct ArrowArrayStream* stream, struct ArrowArray* out_array) {
    ArrowArrayStreamWrapper self{stream};
    return self.ToCError(self.GetNext(out_array));
  }

  static void StaticRelease(struct ArrowArrayStream* stream) {
    ArrowArrayStreamWrapper{stream}.Release();
  }

  static const char* StaticGetLastError(struct ArrowArrayStream* stream) {
    return ArrowArrayStreamWrapper{stream}.GetLastError();
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
        return EINVAL; // Fallback for Invalid, TypeError, etc.
    }
  }

  PrivateData* private_data() const {
    return reinterpret_cast<PrivateData*>(stream_->private_data);
  }

  gluten::TransferIterator* iterator() {
    return private_data()->iterator_.get();
  }

  const std::shared_ptr<arrow::Schema> schema() const {
    return private_data()->schema_;
  }

  struct ArrowArrayStream* stream_;
};

ARROW_EXPORT
std::shared_ptr<struct ArrowArrayStream> inline
CreateArrowArrayStream(
    std::shared_ptr<arrow::Schema> schema,
    std::unique_ptr<TransferIterator>&& iterator) {
  auto out = std::make_shared<struct ArrowArrayStream>();
  out->get_schema = ArrowArrayStreamWrapper::StaticGetSchema;
  out->get_next = ArrowArrayStreamWrapper::StaticGetNext;
  out->get_last_error = ArrowArrayStreamWrapper::StaticGetLastError;
  out->release = ArrowArrayStreamWrapper::StaticRelease;
  out->private_data = new ArrowArrayStreamWrapper::PrivateData{std::move(iterator), std::move(schema)};
  return out;
}

} // namespace gluten
