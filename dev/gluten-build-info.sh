#!/bin/bash

#
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
#

VELOX_HOME="$1"
EXTRA_RESOURCE_DIR="$2"
mkdir -p "$EXTRA_RESOURCE_DIR"
BUILD_INFO="$EXTRA_RESOURCE_DIR"/gluten-build-info.properties

echo_build_properties() {
  echo gluten_version="$3"
  echo java_version="$4"
  echo scala_version="$5"
  echo spark_version="$6"
  echo hadoop_version="$7"
  echo gcc_version=$(gcc --version | head -n 1)
  echo branch=$(git rev-parse --abbrev-ref HEAD)
  echo revision=$(git rev-parse HEAD)
  echo revision_time=$(git show -s --format=%ci HEAD)
  echo date=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  echo url=$(git config --get remote.origin.url)
  if [ -d "$VELOX_HOME" ]; then
    echo velox_branch=$(git -C $VELOX_HOME rev-parse --abbrev-ref HEAD)
    echo velox_revision=$(git -C $VELOX_HOME rev-parse HEAD)
    echo velox_revision_time=$(git -C $VELOX_HOME show -s --format=%ci HEAD)
  fi
}

echo_build_properties "$@" > "$BUILD_INFO"
