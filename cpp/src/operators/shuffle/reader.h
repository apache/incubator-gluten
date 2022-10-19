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

#include "memory/columnar_batch.h"
#include "type.h"

namespace gluten {
namespace shuffle {

class Reader {
 public:
  Reader(
      std::shared_ptr<arrow::io::InputStream> in,
      std::shared_ptr<arrow::Schema> schema,
      gluten::shuffle::ReaderOptions options);

  arrow::Result<std::shared_ptr<gluten::memory::GlutenColumnarBatch>> Next();
  arrow::Status Close();

 private:
  std::shared_ptr<arrow::io::InputStream> in_;
  gluten::shuffle::ReaderOptions options_;
  std::shared_ptr<arrow::Schema> schema_;
  std::unique_ptr<arrow::ipc::Message> first_message_;
  bool first_message_consumed_ = false;
};

} // namespace shuffle
} // namespace gluten
