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

GLUTEN_SOURCE=$(cd $(dirname -- $0)/../../..; pwd -P)

function detect_os_version() {
  if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    OS_DISTRIBUTION=$(cat /etc/os-release | grep "^ID=" | cut -d "=" -f 2 | tr -d '"')
    OS_VERSION_ID=$(cat /etc/os-release | grep "^VERSION_ID=" | cut -d "=" -f 2 | tr -d '"')
  elif [[ "$OSTYPE" == "darwin"* ]]; then
    OS_DISTRIBUTION="macos"
    OS_VERSION_ID=$(sw_vers -productVersion)
  else
    echo "Unsupported OS: $OSTYPE"
    exit 1
  fi
  export OS_VERSION=${OS_DISTRIBUTION}${OS_VERSION_ID}
}
detect_os_version

function get_project_version() {
  cd "${GLUTEN_SOURCE}"
  # use mvn command to get project version
  PROJECT_VERSION=$(mvn -q -Dexec.executable="echo" -Dexec.args='${project.version}' --non-recursive exec:exec)
  export PROJECT_VERSION=${PROJECT_VERSION}
}
get_project_version

BUILD_VERSION=${BUILD_VERSION:-${PROJECT_VERSION}}
OS_VERSION=${OS_VERSION}
OS_ARCH=$(uname -m)
PACKAGE_NAME=gluten-${BUILD_VERSION}-${OS_VERSION}-${OS_ARCH}
PACKAGE_DIR_PATH="${GLUTEN_SOURCE}"/dist/"${PACKAGE_NAME}"

# cleanup working directory
[[ -d "${GLUTEN_SOURCE}"/dist/"${PACKAGE_NAME}" ]] && rm -rf "${GLUTEN_SOURCE}"/dist/"${PACKAGE_NAME}"
[[ -d "${GLUTEN_SOURCE}"/dist/"${PACKAGE_NAME}".tar.gz ]] && rm -f "${GLUTEN_SOURCE}"/dist/"${PACKAGE_NAME}".tar.gz
[[ -d "${GLUTEN_SOURCE}"/cpp-ch/build ]] && rm -rf "${GLUTEN_SOURCE}"/cpp-ch/build
[[ -d "${GLUTEN_SOURCE}"/cpp-ch/build_ch ]] && rm -rf "${GLUTEN_SOURCE}"/cpp-ch/build_ch
[[ -d "${GLUTEN_SOURCE}"/cpp-ch/ClickHouse/build ]] && rm -rf "${GLUTEN_SOURCE}"/cpp-ch/ClickHouse/build
[[ -L "${GLUTEN_SOURCE}"/cpp-ch/ClickHouse/utils/extern-local-engine ]] && rm -f "${GLUTEN_SOURCE}"/cpp-ch/ClickHouse/utils/extern-local-engine

# create package folder
mkdir -p "${GLUTEN_SOURCE}"/dist/"${PACKAGE_NAME}"
mkdir "${GLUTEN_SOURCE}"/dist/"${PACKAGE_NAME}"/bin
mkdir "${GLUTEN_SOURCE}"/dist/"${PACKAGE_NAME}"/conf
mkdir "${GLUTEN_SOURCE}"/dist/"${PACKAGE_NAME}"/jars
mkdir "${GLUTEN_SOURCE}"/dist/"${PACKAGE_NAME}"/extraJars
mkdir "${GLUTEN_SOURCE}"/dist/"${PACKAGE_NAME}"/extraJars/spark32
mkdir "${GLUTEN_SOURCE}"/dist/"${PACKAGE_NAME}"/extraJars/spark33
mkdir "${GLUTEN_SOURCE}"/dist/"${PACKAGE_NAME}"/libs
mkdir "${GLUTEN_SOURCE}"/dist/"${PACKAGE_NAME}"/logs

# create BUILD_INFO
{
  echo "BUILD_VERSION=${BUILD_VERSION}"
  echo "OS_VERSION=${OS_VERSION}"
  echo "OS_ARCH=${OS_ARCH}"
  echo COMMIT_SHA="$(git rev-parse HEAD)"
} > "${GLUTEN_SOURCE}"/dist/"${PACKAGE_NAME}"/BUILD_INFO

# copy LICENSE and README.md
cp "${GLUTEN_SOURCE}"/LICENSE "${GLUTEN_SOURCE}"/dist/"${PACKAGE_NAME}"
cp "${GLUTEN_SOURCE}"/README.md "${GLUTEN_SOURCE}"/dist/"${PACKAGE_NAME}"

# build gluten jar
cd "${GLUTEN_SOURCE}"
mvn clean package -Pbackends-clickhouse -Pspark-3.2 -Prss -DskipTests -Dcheckstyle.skip
mvn clean package -Pspark-3.3 -am -pl shims/spark33 -DskipTests -Dcheckstyle.skip

# build libch.so
bash "${GLUTEN_SOURCE}"/ep/build-clickhouse/src/build_clickhouse.sh

# copy gluten jar and libch.so
cp "${GLUTEN_SOURCE}"/backends-clickhouse/target/gluten-*-jar-with-dependencies-exclude-sparkshims.jar "${PACKAGE_DIR_PATH}"/jars/gluten.jar
cp "${GLUTEN_SOURCE}"/gluten-celeborn/clickhouse/target/gluten-celeborn-clickhouse-${PROJECT_VERSION}-jar-with-dependencies.jar "${PACKAGE_DIR_PATH}"/jars
cp "$GLUTEN_SOURCE"/cpp-ch/build/utils/extern-local-engine/libch.so "${PACKAGE_DIR_PATH}"/libs/libch.so
cp "${GLUTEN_SOURCE}"/shims/spark32/target/spark-*-${PROJECT_VERSION}.jar "${PACKAGE_DIR_PATH}"/extraJars/spark32/spark32-shims.jar
cp "${GLUTEN_SOURCE}"/shims/spark33/target/spark-*-${PROJECT_VERSION}.jar "${PACKAGE_DIR_PATH}"/extraJars/spark33/spark33-shims.jar

# copy bin and conf
cp "${GLUTEN_SOURCE}"/ep/build-clickhouse/src/resources/bin/* "${GLUTEN_SOURCE}"/dist/"${PACKAGE_NAME}"/bin
cp "${GLUTEN_SOURCE}"/ep/build-clickhouse/src/resources/conf/* "${GLUTEN_SOURCE}"/dist/"${PACKAGE_NAME}"/conf

# download 3rd party jars
protobuf_version=$(mvn -q -Dexec.executable="echo" -Dexec.args='${protobuf.version}' --non-recursive exec:exec)
wget https://repo1.maven.org/maven2/com/google/protobuf/protobuf-java/${protobuf_version}/protobuf-java-${protobuf_version}.jar -P "${PACKAGE_DIR_PATH}"/jars
celeborn_version=$(mvn -q -Dexec.executable="echo" -Dexec.args='${celeborn.version}' --non-recursive exec:exec)
wget https://repo1.maven.org/maven2/org/apache/celeborn/celeborn-client-spark-3-shaded_2.12/${celeborn_version}/celeborn-client-spark-3-shaded_2.12-${celeborn_version}.jar -P "${PACKAGE_DIR_PATH}"/jars
delta_version_32=$(mvn -q -Dexec.executable="echo" -Dexec.args='${delta.version}' -Pspark-3.2 --non-recursive exec:exec)
wget https://repo1.maven.org/maven2/io/delta/delta-core_2.12/${delta_version_32}/delta-core_2.12-${delta_version_32}.jar -P "${PACKAGE_DIR_PATH}"/extraJars/spark32
wget https://repo1.maven.org/maven2/io/delta/delta-storage/${delta_version_32}/delta-storage-${delta_version_32}.jar -P "${PACKAGE_DIR_PATH}"/extraJars/spark32
delta_version_33=$(mvn -q -Dexec.executable="echo" -Dexec.args='${delta.version}' -Pspark-3.3 --non-recursive exec:exec)
wget https://repo1.maven.org/maven2/io/delta/delta-core_2.12/${delta_version_33}/delta-core_2.12-${delta_version_33}.jar -P "${GLUTEN_SOURCE}"/dist/"${PACKAGE_NAME}"/extraJars/spark33
wget https://repo1.maven.org/maven2/io/delta/delta-storage/${delta_version_33}/delta-storage-${delta_version_33}.jar -P "${GLUTEN_SOURCE}"/dist/"${PACKAGE_NAME}"/extraJars/spark33

# build tar.gz
cd "${GLUTEN_SOURCE}"/dist
tar -czf "${PACKAGE_NAME}".tar.gz "${PACKAGE_NAME}"

echo "Build package successfully, package path:"
echo "${GLUTEN_SOURCE}"/dist/"${PACKAGE_NAME}".tar.gz

