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

#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <folly/executors/IOThreadPoolExecutor.h>

#include "operators/writer/Datasource.h"

namespace gluten {

class Datasource {
 public:
  Datasource(const std::string& file_path, const std::string& file_name, std::shared_ptr<arrow::Schema> schema)
      : file_path_(file_path), file_name_(file_name), schema_(schema) {}

  virtual void Init(const std::unordered_map<std::string, std::string>& sparkConfs) {}
  virtual std::shared_ptr<arrow::Schema> InspectSchema() {
    return nullptr;
  }
  virtual void Write(const std::shared_ptr<ColumnarBatch>& cb) {}
  virtual void Close() {}
  virtual std::shared_ptr<arrow::Schema> GetSchema() {
    return nullptr;
  }

 private:
  std::string file_path_;
  std::string file_name_;
  std::shared_ptr<arrow::Schema> schema_;
};

} // namespace gluten
