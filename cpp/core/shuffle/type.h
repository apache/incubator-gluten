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

#include <arrow/extension_type.h>
#include <arrow/ipc/options.h>
#include <arrow/type.h>
#include <arrow/util/logging.h>

#include <deque>

#include "jni/JniCommon.h"

#include "memory/ArrowMemoryPool.h"

namespace gluten {

const unsigned kOnes[] = {1, 1, 1, 1, 1, 1, 1, 1};

struct ReaderOptions {
  arrow::ipc::IpcReadOptions ipc_read_options = arrow::ipc::IpcReadOptions::Defaults();

  static ReaderOptions defaults();
};

namespace type {
/// \brief Data type enumeration for shuffle writer
///
/// This enumeration maps the types of arrow::Type::type with same length
/// to identical type

enum typeId : int {
  kShuffle1Byte,
  kShuffle2Byte,
  kShuffle4Byte,
  kShuffle8Byte,
  kShuffleDecimaL128,
  kShuffleBit,
  kShuffleBinary,
  kShuffleLargeBinary,
  kShuffleList,
  kShuffleLargeList,
  kShuffleNull,
  kNumTypes,
  kShuffleNotImplemented
};

static const typeId kAll[] = {
    kShuffle1Byte,
    kShuffle2Byte,
    kShuffle4Byte,
    kShuffle8Byte,
    kShuffleDecimaL128,
    kShuffleBit,
    kShuffleBinary,
    kShuffleLargeBinary,
    kShuffleNull,
};

} // namespace type

} // namespace gluten
