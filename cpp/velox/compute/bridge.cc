
#include "bridge.h"

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
#include "arrow/util/string_view.h"
#include "arrow/util/value_parsing.h"
#include "arrow/visit_type_inline.h"

namespace gluten {


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
