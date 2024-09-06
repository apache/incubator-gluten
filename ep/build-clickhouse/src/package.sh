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

DEFAULT_SPARK_PROFILE="spark-3.3"
function get_project_version() {
  cd "${GLUTEN_SOURCE}"
  # use mvn command to get project version
  PROJECT_VERSION=$(mvn -q -P${DEFAULT_SPARK_PROFILE} -Dexec.executable="echo" -Dexec.args='${project.version}' --non-recursive exec:exec)
  export PROJECT_VERSION=${PROJECT_VERSION}
}
get_project_version

BUILD_VERSION=${BUILD_VERSION:-${PROJECT_VERSION}}
OS_VERSION=${OS_VERSION}
OS_ARCH=$(uname -m)
PACKAGE_NAME=gluten-${BUILD_VERSION}-${OS_VERSION}-${OS_ARCH}
PACKAGE_DIR_PATH="${GLUTEN_SOURCE}"/dist/"${PACKAGE_NAME}"
spark_versions=("3.2" "3.3" "3.5")

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
mkdir "${GLUTEN_SOURCE}"/dist/"${PACKAGE_NAME}"/libs
mkdir "${GLUTEN_SOURCE}"/dist/"${PACKAGE_NAME}"/logs

for sv in "${spark_versions[@]}"
do
    replace_dot=$(echo "$sv" | tr -d '.')
    mkdir "${GLUTEN_SOURCE}"/dist/"${PACKAGE_NAME}"/jars/spark"$replace_dot"
done

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

function build_gluten_by_spark_version() {
  spark_profile=$1
  sv=$(echo "$spark_profile" | tr -d '.')
  echo "build gluten with spark ${spark_profile}"

  mvn clean install -Pbackends-clickhouse -Pspark-"${spark_profile}" -Pceleborn -DskipTests -Dcheckstyle.skip
  cp "${GLUTEN_SOURCE}"/backends-clickhouse/target/gluten-*-spark-"${spark_profile}"-jar-with-dependencies.jar "${PACKAGE_DIR_PATH}"/jars/spark"${sv}"/gluten.jar
  cp "${GLUTEN_SOURCE}"/gluten-celeborn/clickhouse/target/gluten-celeborn-clickhouse-"${PROJECT_VERSION}"-jar-with-dependencies.jar "${PACKAGE_DIR_PATH}"/jars/spark"${sv}"
  delta_version=$(mvn -q -Dexec.executable="echo" -Dexec.args='${delta.version}' -Pspark-"${spark_profile}" --non-recursive exec:exec)
  delta_package_name=$(mvn -q -Dexec.executable="echo" -Dexec.args='${delta.package.name}' -Pspark-"${spark_profile}" --non-recursive exec:exec)
  wget https://repo1.maven.org/maven2/io/delta/"${delta_package_name}"_2.12/"${delta_version}"/"${delta_package_name}"_2.12-"${delta_version}".jar -P "${PACKAGE_DIR_PATH}"/jars/spark"${sv}"
  wget https://repo1.maven.org/maven2/io/delta/delta-storage/"${delta_version}"/delta-storage-"${delta_version}".jar -P "${PACKAGE_DIR_PATH}"/jars/spark"${sv}"
}

# download common 3rd party jars
protobuf_version=$(mvn -q -P${DEFAULT_SPARK_PROFILE} -Dexec.executable="echo" -Dexec.args='${protobuf.version}' --non-recursive exec:exec)
wget https://repo1.maven.org/maven2/com/google/protobuf/protobuf-java/${protobuf_version}/protobuf-java-${protobuf_version}.jar -P "${PACKAGE_DIR_PATH}"/jars/spark32
celeborn_version=$(mvn -q -P${DEFAULT_SPARK_PROFILE} -Dexec.executable="echo" -Dexec.args='${celeborn.version}' --non-recursive exec:exec)
wget https://repo1.maven.org/maven2/org/apache/celeborn/celeborn-client-spark-3-shaded_2.12/${celeborn_version}/celeborn-client-spark-3-shaded_2.12-${celeborn_version}.jar -P "${PACKAGE_DIR_PATH}"/jars/spark32

for sv in "${spark_versions[@]}"
do
    build_gluten_by_spark_version "$sv"
    replace_dot=$(echo "$sv" | tr -d '.')
    if [[ "$replace_dot" == "32" ]];then
        continue # error: xxx are the same file
    fi
    cp -f "${PACKAGE_DIR_PATH}"/jars/spark32/protobuf-java-"${protobuf_version}".jar "${PACKAGE_DIR_PATH}"/jars/spark"${replace_dot}"
    cp -f "${PACKAGE_DIR_PATH}"/jars/spark32/celeborn-client-spark-3-shaded_2.12-"${celeborn_version}".jar "${PACKAGE_DIR_PATH}"/jars/spark"${replace_dot}"
done

# build libch.so
bash "${GLUTEN_SOURCE}"/ep/build-clickhouse/src/build_clickhouse.sh
cp "$GLUTEN_SOURCE"/cpp-ch/build/utils/extern-local-engine/libch.so "${PACKAGE_DIR_PATH}"/libs/libch.so

# copy bin and conf
cp "${GLUTEN_SOURCE}"/ep/build-clickhouse/src/resources/bin/* "${GLUTEN_SOURCE}"/dist/"${PACKAGE_NAME}"/bin
cp "${GLUTEN_SOURCE}"/ep/build-clickhouse/src/resources/conf/* "${GLUTEN_SOURCE}"/dist/"${PACKAGE_NAME}"/conf

# build tar.gz
cd "${GLUTEN_SOURCE}"/dist
tar -czf "${PACKAGE_NAME}".tar.gz "${PACKAGE_NAME}"

echo "Build package successfully, package path:"
echo "${GLUTEN_SOURCE}"/dist/"${PACKAGE_NAME}".tar.gz

