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

GLUTEN_ROOT=$(cd $(dirname -- $0)/..; pwd -P)

BACKEND_TYPE="$1"
BACKEND_HOME="$2"
EXTRA_RESOURCE_DIR="$3"
mkdir -p "$EXTRA_RESOURCE_DIR"
BUILD_INFO="$EXTRA_RESOURCE_DIR"/gluten-build-info.properties

echo_build_properties() {
  echo gluten_version="$4"
  echo backend_type="$BACKEND_TYPE"
  echo java_version="$5"
  echo scala_version="$6"
  echo spark_version="$7"
  echo hadoop_version="$8"
  echo branch=$(git rev-parse --abbrev-ref HEAD)
  echo revision=$(git rev-parse HEAD)
  echo revision_time=$(git show -s --format=%ci HEAD)
  echo date=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  echo url=$(git config --get remote.origin.url)

  if [ "$BACKEND_TYPE" = "velox" ]; then
      echo gcc_version=$(strings $GLUTEN_ROOT/cpp/build/releases/libgluten.so | grep "GCC:" | head -n 1)
      echo velox_branch=$(git -C $BACKEND_HOME rev-parse --abbrev-ref HEAD)
      echo velox_revision=$(git -C $BACKEND_HOME rev-parse HEAD)
      echo velox_revision_time=$(git -C $BACKEND_HOME show -s --format=%ci HEAD)
  fi
  if [ "$BACKEND_TYPE" = "ch"  ]; then
      echo ch_org=$(cat $GLUTEN_ROOT/cpp-ch/clickhouse.version | grep -oP '(?<=^CH_ORG=).*')
      echo ch_branch=$(cat $GLUTEN_ROOT/cpp-ch/clickhouse.version | grep -oP '(?<=^CH_BRANCH=).*')
      echo ch_commit=$(cat $GLUTEN_ROOT/cpp-ch/clickhouse.version | grep -oP '(?<=^CH_COMMIT=).*')
  fi
}

echo_build_properties "$@" > "$BUILD_INFO"
