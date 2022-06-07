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

#include <arrow/type_fwd.h>

#include <folly/executors/IOThreadPoolExecutor.h>

#include "velox/common/file/FileSystems.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"

using namespace facebook::velox;

namespace velox {
namespace compute {

class DwrfDatasource {
 public:
  DwrfDatasource(const std::string& file_path) : file_path_(file_path) {}

  void Init();
  std::shared_ptr<arrow::Schema> InspectSchema();
  void Close();

 private:
  std::string file_path_;
};

}  // namespace compute
}  // namespace velox
