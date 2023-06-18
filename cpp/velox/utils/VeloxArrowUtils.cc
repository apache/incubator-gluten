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

#include "utils/VeloxArrowUtils.h"

#include <ComplexVector.h>
#include <velox/vector/arrow/Bridge.h>
#include "memory/VeloxColumnarBatch.h"

namespace gluten {

arrow::Result<std::shared_ptr<ColumnarBatch>> recordBatch2VeloxColumnarBatch(const arrow::RecordBatch& rb) {
  ArrowArray arrowArray;
  ArrowSchema arrowSchema;
  RETURN_NOT_OK(arrow::ExportRecordBatch(rb, &arrowArray, &arrowSchema));
  auto vp =
      facebook::velox::importFromArrowAsOwner(arrowSchema, arrowArray, gluten::defaultLeafVeloxMemoryPool().get());
  return std::make_shared<VeloxColumnarBatch>(std::dynamic_pointer_cast<facebook::velox::RowVector>(vp));
}

uint64_t parseMemoryEnv(const std::string& envStr) {
  const char* memoryEnv = std::getenv(envStr.c_str());
  if (memoryEnv != nullptr) {
    std::string memory = memoryEnv;
    uint64_t shift = 0;

    switch (memory.back()) {
      case 'G':
      case 'g':
        shift = 30;
        memory.pop_back();
        break;
      case 'M':
      case 'm':
        shift = 20;
        memory.pop_back();
        break;
      default:
        std::cerr << "Memory value should have a G, g, M or m suffix: " << memoryEnv << std::endl;
        return std::numeric_limits<uint64_t>::max();
    }

    try {
      return std::stoul(memory) << shift;
    } catch (const std::invalid_argument& e) {
      std::cerr << "Invalid memory format: " << memoryEnv << std::endl;
    } catch (const std::out_of_range& e) {
      std::cerr << "Memory value out of range: " << memoryEnv << std::endl;
    }
  }
  return std::numeric_limits<uint64_t>::max();
}

} // namespace gluten
