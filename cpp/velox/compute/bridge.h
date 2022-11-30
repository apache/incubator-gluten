#pragma once

#include "arrow/util/iterator.h"
#include "releases/include/arrow/c/bridge.h"

namespace gluten {
using ArrowArrayIterator = arrow::Iterator<std::shared_ptr<ArrowArray>>;
}
namespace arrow {
ARROW_EXPORT
Status ExportArrowArray(
    std::shared_ptr<arrow::Schema> schema,
    std::shared_ptr<gluten::ArrowArrayIterator> reader,
    struct ArrowArrayStream* out);
} // namespace arrow
