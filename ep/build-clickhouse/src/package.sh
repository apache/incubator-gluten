#!/bin/bash

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
SPARK_MAJOR_MINOR_VERSION=${SPARK_MAJOR_MINOR_VERSION:-3.2}
OS_VERSION=${OS_VERSION}
OS_ARCH=$(uname -m)
PACKAGE_NAME=gluten-${BUILD_VERSION}-spark${SPARK_MAJOR_MINOR_VERSION}-${OS_VERSION}-${OS_ARCH}

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

# create BUILD_INFO
{
  echo "BUILD_VERSION=${BUILD_VERSION}"
  echo "SPARK_MAJOR_MINOR_VERSION=${SPARK_MAJOR_MINOR_VERSION}"
  echo "OS_VERSION=${OS_VERSION}"
  echo "OS_ARCH=${OS_ARCH}"
  echo COMMIT_SHA="$(git rev-parse HEAD)"
} > "${GLUTEN_SOURCE}"/dist/"${PACKAGE_NAME}"/BUILD_INFO

# copy LICENSE and README.md
cp "${GLUTEN_SOURCE}"/LICENSE "${GLUTEN_SOURCE}"/dist/"${PACKAGE_NAME}"
cp "${GLUTEN_SOURCE}"/README.md "${GLUTEN_SOURCE}"/dist/"${PACKAGE_NAME}"

# build gluten jar
mvn clean package -Pbackends-clickhouse -Pspark-"${SPARK_MAJOR_MINOR_VERSION}" -DskipTests -Dcheckstyle.skip

# build libch.so
bash "${GLUTEN_SOURCE}"/ep/build-clickhouse/src/build_clickhouse.sh

# copy gluten jar and libch.so
cp "${GLUTEN_SOURCE}"/backends-clickhouse/target/gluten-*-jar-with-dependencies.jar "${GLUTEN_SOURCE}"/dist/"${PACKAGE_NAME}"/jars/gluten.jar
cp "$GLUTEN_SOURCE"/cpp-ch/build/utils/extern-local-engine/libch.so "${GLUTEN_SOURCE}"/dist/"${PACKAGE_NAME}"/libs/libch.so

# copy bin and conf
cp "${GLUTEN_SOURCE}"/ep/build-clickhouse/src/resources/bin/* "${GLUTEN_SOURCE}"/dist/"${PACKAGE_NAME}"/bin
cp "${GLUTEN_SOURCE}"/ep/build-clickhouse/src/resources/conf/* "${GLUTEN_SOURCE}"/dist/"${PACKAGE_NAME}"/conf

# download 3rd party jars
if [[ "${SPARK_MAJOR_MINOR_VERSION}" == "3.2" ]]; then
  wget https://repo1.maven.org/maven2/com/google/protobuf/protobuf-java/3.16.3/protobuf-java-3.16.3.jar -P "${GLUTEN_SOURCE}"/dist/"${PACKAGE_NAME}"/jars
  wget https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.0.1/delta-core_2.12-2.0.1.jar -P "${GLUTEN_SOURCE}"/dist/"${PACKAGE_NAME}"/jars
  wget https://repo1.maven.org/maven2/io/delta/delta-storage/2.0.1/delta-storage-2.0.1.jar -P "${GLUTEN_SOURCE}"/dist/"${PACKAGE_NAME}"/jars
elif [ "${SPARK_MAJOR_MINOR_VERSION}" == "3.3" ]; then
  wget https://repo1.maven.org/maven2/com/google/protobuf/protobuf-java/3.16.3/protobuf-java-3.16.3.jar -P "${GLUTEN_SOURCE}"/dist/"${PACKAGE_NAME}"/jars
  wget https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.2.0/delta-core_2.12-2.2.0.jar -P "${GLUTEN_SOURCE}"/dist/"${PACKAGE_NAME}"/jars
  wget https://repo1.maven.org/maven2/io/delta/delta-storage/2.2.0/delta-storage-2.2.0.jar -P "${GLUTEN_SOURCE}"/dist/"${PACKAGE_NAME}"/jars
else
  echo "Unsupported spark version: ${SPARK_MAJOR_MINOR_VERSION}"
  exit 1
fi

# build tar.gz
cd "${GLUTEN_SOURCE}"/dist
tar -czf "${PACKAGE_NAME}".tar.gz "${PACKAGE_NAME}"

echo "Build package successfully, package path:"
echo "${GLUTEN_SOURCE}"/dist/"${PACKAGE_NAME}".tar.gz

