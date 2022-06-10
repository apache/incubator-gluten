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

#include "DwrfDatasource.h"

#include "ArrowTypeUtils.h"

using namespace facebook::velox;
using namespace facebook::velox::dwio::common;

namespace velox {
namespace compute {

void DwrfDatasource::Init() {
  // Setup and register.
  filesystems::registerLocalFileSystem();
  try {
    dwrf::registerDwrfReaderFactory();
  } catch (const VeloxRuntimeError& e) {
    // The reader factory is already registered in local mode and no need to register.
    return;
  }
}

std::shared_ptr<arrow::Schema> DwrfDatasource::InspectSchema() {
  dwio::common::ReaderOptions reader_options;
  auto format = dwio::common::FileFormat::ORC;  // DWRF
  reader_options.setFileFormat(format);

  if (strncmp(file_path_.c_str(), "file:", 5) == 0) {
    std::unique_ptr<dwio::common::Reader> reader =
        dwio::common::getReaderFactory(reader_options.getFileFormat())
            ->createReader(
                std::make_unique<dwio::common::FileInputStream>(file_path_.substr(5)),
                reader_options);
    return toArrowSchema(reader->rowType());
  } else {
    throw std::runtime_error(
        "The path is not local file path when inspect shcema with DWRF format!");
  }
}

void DwrfDatasource::Close() { dwrf::unregisterDwrfReaderFactory; }

}  // namespace compute
}  // namespace velox
