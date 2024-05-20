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

set -exu

NPROC=$(getconf _NPROCESSORS_ONLN)

function wget_and_untar {
  local URL=$1
  local DIR=$2
  mkdir -p "${DIR}"
  pushd "${DIR}"
  curl -L "${URL}" > $2.tar.gz
  tar -xz --strip-components=1 -f $2.tar.gz
  popd
}

function install_openssl {
  wget_and_untar https://github.com/openssl/openssl/archive/refs/tags/OpenSSL_1_1_1s.tar.gz openssl
 cd openssl
 ./config no-shared && make depend && make && sudo make install
 cd ..
}

function install_arrow_deps {
  install_openssl
}

# Activate gcc9; enable errors on unset variables afterwards.
# source /opt/rh/gcc-toolset-9/enable || exit 1
install_arrow_deps
echo "All dependencies for Arrow installed!"
