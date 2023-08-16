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

#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/c/helpers.h>

#include "ArrowTypeUtils.h"
#include "memory/VeloxMemoryPool.h"
#include "utils/exception.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/arrow/Bridge.h"

using namespace facebook;

namespace gluten {

void toArrowSchema(const velox::TypePtr& rowType, struct ArrowSchema* out) {
  exportToArrow(velox::BaseVector::create(rowType, 0, defaultLeafVeloxMemoryPool().get()), *out);
}

std::shared_ptr<arrow::Schema> toArrowSchema(const velox::TypePtr& rowType) {
  ArrowSchema arrowSchema;
  toArrowSchema(rowType, &arrowSchema);
  GLUTEN_ASSIGN_OR_THROW(auto outputSchema, arrow::ImportSchema(&arrowSchema));
  return outputSchema;
}

facebook::velox::TypePtr fromArrowSchema(const std::shared_ptr<arrow::Schema>& schema) {
  ArrowSchema cSchema;
  GLUTEN_THROW_NOT_OK(arrow::ExportSchema(*schema, &cSchema));
  facebook::velox::TypePtr typePtr = facebook::velox::importFromArrow(cSchema);
  // It should be facebook::velox::importFromArrow's duty to release the imported arrow c schema.
  // Since exported Velox type prt doesn't hold memory from the c schema.
  ArrowSchemaRelease(&cSchema); // otherwise the c schema leaks memory
  return typePtr;
}
} // namespace gluten
