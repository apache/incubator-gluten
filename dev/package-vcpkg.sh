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

set -eux

CURRENT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
GLUTEN_DIR="$CURRENT_DIR/.."
LINUX_OS=$(. /etc/os-release && echo ${ID})
VERSION=$(. /etc/os-release && echo ${VERSION_ID})
ARCH=`uname -m`

cd "$GLUTEN_DIR"
if [ "$LINUX_OS" == "centos" ]; then
  if [ "$VERSION" == "8" ]; then
    source /opt/rh/gcc-toolset-11/enable
  elif [ "$VERSION" == "7" ]; then
    export MANPATH=""
    source /opt/rh/devtoolset-11/enable
  fi
fi


# build gluten with velox backend, prompt always respond y
export PROMPT_ALWAYS_RESPOND=y

./dev/buildbundle-veloxbe.sh --enable_vcpkg=ON --build_tests=ON --build_arrow=OFF --build_benchmarks=ON --enable_s3=ON --enable_gcs=ON --enable_hdfs=ON "$@"
