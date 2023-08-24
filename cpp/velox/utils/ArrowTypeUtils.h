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

#pragma once

#include <arrow/type.h>

#include "velox/type/Type.h"

namespace gluten {

// FIXME: The namings should emphasize "Velox" as well
void toArrowSchema(const facebook::velox::TypePtr& rowType, struct ArrowSchema* out);

std::shared_ptr<arrow::Schema> toArrowSchema(const facebook::velox::TypePtr& rowType);

facebook::velox::TypePtr fromArrowSchema(const std::shared_ptr<arrow::Schema>& schema);

} // namespace gluten
