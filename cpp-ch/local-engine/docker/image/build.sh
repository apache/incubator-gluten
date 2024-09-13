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

mkdir -p /build && cd /build || exit
export CCACHE_DIR=/ccache
export CCACHE_BASEDIR=/build
export CCACHE_NOHASHDIR=true
export CCACHE_COMPILERCHECK=content
export CCACHE_MAXSIZE=15G

# link the local-engine lib to a subdir of clickhouse
rm -f /clickhosue/util/extern-local-engine
ln -s /gluten/cpp-ch/local-engine /clickhouse/utils/extern-local-engine

cmake -G Ninja  "-DCMAKE_C_COMPILER=$CC" "-DCMAKE_CXX_COMPILER=$CXX" \
          "-DCMAKE_BUILD_TYPE=Release" \
          "-DENABLE_PROTOBUF=1" \
          "-DENABLE_EMBEDDED_COMPILER=$ENABLE_EMBEDDED_COMPILER" \
          "-DENABLE_TESTS=OFF" \
          "-DWERROR=OFF" \
          "-DENABLE_JEMALLOC=1" \
          "-DENABLE_MULTITARGET_CODE=ON" \
          "-DENABLE_GWP_ASAN=OFF" \
          "-DENABLE_EXTERN_LOCAL_ENGINE=ON" \
          "-DENABLE_THINLTO=false" \
          /clickhouse
ninja

cp /build/utils/extern-local-engine/libch.so "/output/libch_$(date +%Y%m%d).so"
