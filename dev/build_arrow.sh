#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

CURRENT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
source ${CURRENT_DIR}/build_helper_functions.sh
VELOX_ARROW_BUILD_VERSION=15.0.0
ARROW_PREFIX=$CURRENT_DIR/arrow_ep
# Always uses BUNDLED in case of that thrift is not installed.
THRIFT_SOURCE="BUNDLED"
BUILD_TYPE=Release
if [ -n "$1" ]; then
  BUILD_TYPE=$1
fi
wget_and_untar https://archive.apache.org/dist/arrow/arrow-${VELOX_ARROW_BUILD_VERSION}/apache-arrow-${VELOX_ARROW_BUILD_VERSION}.tar.gz arrow_ep

git apply $CURRENT_DIR/../ep/build_velox/src/modify_arrow.patch

pushd $ARROW_PREFIX/cpp
cmake -DARROW_PARQUET=ON \
      -DARROW_FILESYSTEM=ON \
      -DARROW_PROTOBUF_USE_SHARED=OFF \
      -DARROW_WITH_THRIFT=ON \
      -DARROW_WITH_LZ4=ON \
      -DARROW_WITH_SNAPPY=ON \
      -DARROW_WITH_ZLIB=ON \
      -DARROW_WITH_ZSTD=ON \
      -DARROW_JEMALLOC=OFF \
      -DARROW_SIMD_LEVEL=NONE \
      -DARROW_RUNTIME_SIMD_LEVEL=NONE \
      -DARROW_WITH_UTF8PROC=OFF \
      -DARROW_TESTING=ON \
      -DCMAKE_INSTALL_PREFIX=${ARROW_PREFIX}/install \
      -DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
      -DARROW_BUILD_STATIC=ON \
      -DThrift_SOURCE=${THRIFT_SOURCE}
make -j
make install
export ARROW_EP_PATH=$ARROW_PREFIX
popd
