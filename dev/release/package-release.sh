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

# Note: Manually create $GLUTEN_HOME/release/ and place the release JARs inside.
#       Provide the release tag (e.g., v1.5.0-rc0) as an argument to this script.

set -eu

usage() {
  echo "Usage: $0 <release_tag>  e.g., v1.5.0-rc0"
  exit 1
}

TAG="${1:-}"; [[ -n "$TAG" ]] || usage

TAG_VERSION=${TAG#v}

RELEASE_VERSION=${TAG_VERSION%-rc*}

CURRENT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
GLUTEN_HOME=${CURRENT_DIR}/../../
if [ ! -d "$GLUTEN_HOME/release/" ]; then
  echo "Release directory does not exist."
fi

pushd $GLUTEN_HOME/release/

SPARK_VERSIONS="3.2 3.3 3.4 3.5"

for v in $SPARK_VERSIONS; do
  JAR="gluten-velox-bundle-spark${v}_2.12-linux_amd64-${RELEASE_VERSION}.jar"
  if [[ ! -f "$JAR" ]]; then
    echo "Missing Gluten release JAR under $GLUTEN_HOME/release/ for Spark $v: $JAR"
    exit 1
  fi
  echo "Packaging for Spark $v..."
  tar -czf apache-gluten-${RELEASE_VERSION}-incubating-bin-spark-${v}.tar.gz \
      ${GLUTEN_HOME}/DISCLAIMER \
      $JAR
done

SRC_ZIP="${TAG}.zip"
SRC_DIR="incubator-gluten-${RELEASE_VERSION}"

echo "Packaging source code..."
wget https://github.com/apache/incubator-gluten/archive/refs/tags/${SRC_ZIP}
unzip -q ${SRC_ZIP}

# Rename folder to remove "rc*" for formal release.
mv incubator-gluten-${TAG_VERSION} ${SRC_DIR}
tar -czf apache-gluten-${RELEASE_VERSION}-incubating-src.tar.gz ${SRC_DIR}
rm -r ${SRC_ZIP} ${SRC_DIR}

popd

echo "Finished packaging release binaries and source code."
