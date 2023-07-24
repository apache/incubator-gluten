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

VELOX_REPO=https://github.com/oap-project/velox.git
VELOX_BRANCH=update
VELOX_HOME=""

#Set on run gluten on HDFS
ENABLE_HDFS=OFF
#It can be set to OFF when compiling velox again
BUILD_PROTOBUF=ON
#Set on run gluten on S3
ENABLE_S3=OFF

LINUX_DISTRIBUTION=$(. /etc/os-release && echo ${ID})
LINUX_VERSION_ID=$(. /etc/os-release && echo ${VERSION_ID})

for arg in "$@"; do
  case $arg in
  --velox_repo=*)
    VELOX_REPO=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --velox_branch=*)
    VELOX_BRANCH=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --velox_home=*)
    VELOX_HOME=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --build_protobuf=*)
    BUILD_PROTOBUF=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --enable_hdfs=*)
    ENABLE_HDFS=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --enable_s3=*)
    ENABLE_S3=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  *)
    OTHER_ARGUMENTS+=("$1")
    shift # Remove generic argument from processing
    ;;
  esac
done

function process_setup_ubuntu {
  # make this function Reentrantly
  git checkout scripts/setup-ubuntu.sh
  sed -i '/libprotobuf-dev/d' scripts/setup-ubuntu.sh
  sed -i '/protobuf-compiler/d' scripts/setup-ubuntu.sh
  sed -i '/^sudo --preserve-env apt update && sudo apt install -y/a\  *thrift* \\' scripts/setup-ubuntu.sh
  sed -i '/^sudo --preserve-env apt update && sudo apt install -y/a\  libiberty-dev \\' scripts/setup-ubuntu.sh
  sed -i '/^sudo --preserve-env apt update && sudo apt install -y/a\  libxml2-dev \\' scripts/setup-ubuntu.sh
  sed -i '/^sudo --preserve-env apt update && sudo apt install -y/a\  libkrb5-dev \\' scripts/setup-ubuntu.sh
  sed -i '/^sudo --preserve-env apt update && sudo apt install -y/a\  libgsasl7-dev \\' scripts/setup-ubuntu.sh
  sed -i '/^sudo --preserve-env apt update && sudo apt install -y/a\  libuuid1 \\' scripts/setup-ubuntu.sh
  sed -i '/^sudo --preserve-env apt update && sudo apt install -y/a\  uuid-dev \\' scripts/setup-ubuntu.sh
  sed -i 's/^  liblzo2-dev.*/  liblzo2-dev \\/g' scripts/setup-ubuntu.sh
  sed -i '/libre2-dev/d' scripts/setup-ubuntu.sh
  sed -i '/libgmock-dev/d' scripts/setup-ubuntu.sh # resolved by ep/build-velox/build/velox_ep/CMake/resolve_dependency_modules/gtest.cmake
  if [ $ENABLE_HDFS == "ON" ]; then
    sed -i '/^function install_fmt.*/i function install_libhdfs3 {\n  github_checkout oap-project/libhdfs3 master \n cmake_install\n}\n' scripts/setup-ubuntu.sh
    sed -i '/^  run_and_time install_fmt/a \ \ run_and_time install_libhdfs3' scripts/setup-ubuntu.sh
    sed -i '/^sudo --preserve-env apt update && sudo apt install -y/a\  yasm \\' scripts/setup-ubuntu.sh
  fi
  if [ $BUILD_PROTOBUF == "ON" ]; then
    sed -i '/^function install_fmt.*/i function install_protobuf {\n  wget https://github.com/protocolbuffers/protobuf/releases/download/v21.4/protobuf-all-21.4.tar.gz\n  tar -xzf protobuf-all-21.4.tar.gz\n  cd protobuf-21.4\n  ./configure  CXXFLAGS="-fPIC"  --prefix=/usr/local\n  make "-j$(nproc)"\n  sudo make install\n  sudo ldconfig\n}\n' scripts/setup-ubuntu.sh
    sed -i '/^  run_and_time install_fmt/a \ \ run_and_time install_protobuf' scripts/setup-ubuntu.sh
  fi
  if [ $ENABLE_S3 == "ON" ]; then
    sed -i '/^function install_fmt.*/i function install_awssdk {\n  github_checkout aws/aws-sdk-cpp 1.9.379 --depth 1 --recurse-submodules\n  cmake_install -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS:BOOL=OFF -DMINIMIZE_SIZE:BOOL=ON -DENABLE_TESTING:BOOL=OFF -DBUILD_ONLY:STRING="s3;identity-management" \n} \n' scripts/setup-ubuntu.sh
    sed -i '/^  run_and_time install_fmt/a \ \ run_and_time install_awssdk' scripts/setup-ubuntu.sh
  fi
  sed -i 's/run_and_time install_conda/#run_and_time install_conda/' scripts/setup-ubuntu.sh

}

function process_setup_centos8 {
  # make this function Reentrantly
  git checkout scripts/setup-centos8.sh
  sed -i '/^function dnf_install/i\DEPENDENCY_DIR=${DEPENDENCY_DIR:-$(pwd)}' scripts/setup-centos8.sh
  sed -i '/^dnf_install autoconf/a\dnf_install libxml2-devel libgsasl-devel libuuid-devel' scripts/setup-centos8.sh

  # install gtest
  sed -i '/^cmake_install_deps fmt/a \ \ install_gtest' scripts/setup-centos8.sh
  sed -i '/^cmake_install_deps fmt/a \install_folly' scripts/setup-centos8.sh

  if [ $ENABLE_HDFS == "ON" ]; then
    sed -i '/^function install_protobuf.*/i function install_libhdfs3 {\n cd "\${DEPENDENCY_DIR}"\n github_checkout oap-project/libhdfs3 master \n cmake_install\n}\n' scripts/setup-centos8.sh
    sed -i '/^cmake_install_deps fmt/a \ \ install_libhdfs3' scripts/setup-centos8.sh
    sed -i '/^dnf_install ninja-build/a\ \ yasm \\' scripts/setup-centos8.sh
  fi
  if [[ $BUILD_PROTOBUF == "ON" ]] || [[ $ENABLE_HDFS == "ON" ]]; then
    sed -i '/^cmake_install_deps fmt/a \ \ install_protobuf' scripts/setup-centos8.sh
  fi
  if [ $ENABLE_S3 == "ON" ]; then
    sed -i '/^cmake_install_deps fmt/a \ \ install_awssdk' scripts/setup-centos8.sh
  fi
}

function process_setup_centos7 {
  # make this function Reentrantly
  git checkout scripts/setup-centos7.sh

  # cmake 3 and ninja should be installed
  sed -i '/^run_and_time install_cmake/d' scripts/setup-centos7.sh
  sed -i '/^run_and_time install_ninja/d' scripts/setup-centos7.sh

  # install gtest
  sed -i '/^  run_and_time install_fmt/a \ \ run_and_time install_gtest' scripts/setup-centos7.sh

  if [ $ENABLE_HDFS = "ON" ]; then
    sed -i '/^function install_protobuf.*/i function install_libhdfs3 {\n cd "\${DEPENDENCY_DIR}"\n github_checkout oap-project/libhdfs3 master \n cmake_install\n}\n' scripts/setup-centos7.sh
    sed -i '/^  run_and_time install_fmt/a \ \ run_and_time install_libhdfs3' scripts/setup-centos7.sh
    sed -i '/^dnf_install ccache/a\ \ yasm \\' scripts/setup-centos7.sh
  fi
  if [[ $BUILD_PROTOBUF == "ON" ]] || [[ $ENABLE_HDFS == "ON" ]]; then
    sed -i '/^  run_and_time install_fmt/a \ \ run_and_time install_protobuf' scripts/setup-centos7.sh
  fi
  if [ $ENABLE_S3 == "ON" ]; then
    sed -i '/^  run_and_time install_fmt/a \ \ run_and_time install_awssdk' scripts/setup-centos7.sh
  fi
}

function process_setup_alinux3 {
  process_setup_centos8
  sed -i "s/.*dnf_install epel-release/#&/" scripts/setup-centos8.sh
  sed -i "s/.*dnf config-manager --set-enabled powertools/#&/" scripts/setup-centos8.sh
  sed -i "s/gcc-toolset-9 //" scripts/setup-centos8.sh
  sed -i "s/.*source \/opt\/rh\/gcc-toolset-9\/enable/#&/" scripts/setup-centos8.sh
  sed -i "s/\${CMAKE_INSTALL_LIBDIR}/lib64/" third_party/CMakeLists.txt
}

echo "Preparing Velox source code..."
echo "ENABLE_HDFS=${ENABLE_HDFS}"
echo "BUILD_PROTOBUF=${BUILD_PROTOBUF}"

CURRENT_DIR=$(
  cd "$(dirname "$BASH_SOURCE")"
  pwd
)

if [ "$VELOX_HOME" == "" ]; then
  VELOX_HOME="$CURRENT_DIR/../build/velox_ep"
fi
VELOX_SOURCE_DIR="${VELOX_HOME}"

# checkout code
TARGET_BUILD_COMMIT="$(git ls-remote $VELOX_REPO $VELOX_BRANCH | awk '{print $1;}')"
if [ -d $VELOX_SOURCE_DIR ]; then
  echo "Velox source folder $VELOX_SOURCE_DIR already exists..."
  cd $VELOX_SOURCE_DIR
  git init .
  EXISTS=$(git show-ref refs/tags/build_$TARGET_BUILD_COMMIT || true)
  if [ -z "$EXISTS" ]; then
    git fetch $VELOX_REPO $TARGET_BUILD_COMMIT:refs/tags/build_$TARGET_BUILD_COMMIT
  fi
  git reset --hard HEAD
  git checkout refs/tags/build_$TARGET_BUILD_COMMIT
else
  git clone $VELOX_REPO -b $VELOX_BRANCH $VELOX_SOURCE_DIR
  cd $VELOX_SOURCE_DIR
  git checkout $TARGET_BUILD_COMMIT
fi
#sync submodules
git submodule sync --recursive
git submodule update --init --recursive

# apply patches
sed -i 's/^  ninja -C "${BINARY_DIR}" install/  sudo ninja -C "${BINARY_DIR}" install/g' scripts/setup-helper-functions.sh
sed -i 's/-mavx2 -mfma -mavx -mf16c -mlzcnt -std=c++17/-march=native -std=c++17 -mno-avx512f/g' scripts/setup-helper-functions.sh
if [[ "$LINUX_DISTRIBUTION" == "ubuntu" || "$LINUX_DISTRIBUTION" == "debian" || "$LINUX_DISTRIBUTION" == "pop" ]]; then
  process_setup_ubuntu
elif [[ "$LINUX_DISTRIBUTION" == "centos" ]]; then
  case "$LINUX_VERSION_ID" in
    8) process_setup_centos8 ;;
    7) process_setup_centos7 ;;
    *)
      echo "Unsupport centos version: $LINUX_VERSION_ID"
      exit 1
    ;;
  esac
elif [[ "$LINUX_DISTRIBUTION" == "alinux" ]]; then
  case "$LINUX_VERSION_ID" in
    3) process_setup_alinux3 ;;
    *)
      echo "Unsupport alinux version: $LINUX_VERSION_ID"
      exit 1
    ;;
  esac
else
  echo "Unsupport linux distribution: $LINUX_DISTRIBUTION"
  exit 1
fi

echo "Velox-get finished."
