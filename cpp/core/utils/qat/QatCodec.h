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

#include <arrow/util/compression.h>
#include <qatzip.h>
#include <vector>

namespace gluten {
namespace qat {

static const std::vector<std::string> kQatSupportedCodec = {"gzip", "zstd"};

bool supportsCodec(const std::string& qatCodec);

std::unique_ptr<arrow::util::Codec> makeQatGZipCodec(QzPollingMode_T pollingMode, int compressionLevel);

std::unique_ptr<arrow::util::Codec> makeDefaultQatGZipCodec();

std::unique_ptr<arrow::util::Codec> makeQatZstdCodec(int compressionLevel);

std::unique_ptr<arrow::util::Codec> makeDefaultQatZstdCodec();
} // namespace qat
} // namespace gluten
