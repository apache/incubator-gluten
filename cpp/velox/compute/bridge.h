#pragma once

#include "arrow/util/iterator.h"
#include "releases/include/arrow/c/bridge.h"

namespace gluten {

using ArrowArrayIterator = arrow::Iterator<std::shared_ptr<ArrowArray>>;

ARROW_EXPORT
arrow::Status ExportArrowArray(
    std::shared_ptr<arrow::Schema> schema,
    std::shared_ptr<ArrowArrayIterator> reader,
    struct ArrowArrayStream* out);

} // namespace gluten
