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

#include "ArrowTypeUtils.h"
#include "memory/VeloxMemoryPool.h"
#include "utils/exception.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/arrow/Bridge.h"

using namespace facebook;

namespace gluten {

std::shared_ptr<arrow::Schema> toArrowSchema(const std::shared_ptr<const velox::RowType>& rowType) {
  ArrowSchema arrowSchema{};
  exportToArrow(velox::BaseVector::create(rowType, 0, getDefaultVeloxLeafMemoryPool().get()), arrowSchema);
  GLUTEN_ASSIGN_OR_THROW(auto outputSchema, arrow::ImportSchema(&arrowSchema));
  return outputSchema;
}

} // namespace gluten