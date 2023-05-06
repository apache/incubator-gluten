#pragma once

#include "arrow/c/bridge.h"
#include "arrow/util/iterator.h"

namespace gluten {

using ArrowArrayIterator = arrow::Iterator<std::shared_ptr<ArrowArray>>;

ARROW_EXPORT
arrow::Status exportArrowArray(
    std::shared_ptr<arrow::Schema> schema,
    std::shared_ptr<ArrowArrayIterator> reader,
    struct ArrowArrayStream* out);

} // namespace gluten
