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

#include <memory>

#include "memory/columnar_batch.h"

#ifdef GLUTEN_PRINT_DEBUG
#include <iostream>
#endif

namespace gluten {

class TransferIterator {
 public:
  TransferIterator(
       void* raw_iterator,
       std::unique_ptr<void, void (*)(void*)> iterator,
       std::shared_ptr<ColumnarBatch> (*iterator_next_fn)(void*)) 
     : raw_iterator_(raw_iterator), iterator_(std::move(iterator)), iterator_next_fn_(iterator_next_fn) {}

  std::shared_ptr<ArrowArray> Next() {
     std::shared_ptr<ColumnarBatch> cb = (*iterator_next_fn_)(raw_iterator_);
     if (cb) {
       return cb->exportArrowArray();
     }
     return nullptr;
  }

 private:
  void* raw_iterator_;
  std::unique_ptr<void, void (*)(void*)> iterator_;
  std::shared_ptr<ColumnarBatch> (*iterator_next_fn_)(void*);
};

} // namespace gluten
