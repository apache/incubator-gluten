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
VELOX_BRANCH=2024_07_29
VELOX_HOME=""

OS=`uname -s`

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
  *)
    OTHER_ARGUMENTS+=("$1")
    shift # Remove generic argument from processing
    ;;
  esac
done

function ensure_pattern_matched {
  if [ $# -ne 2 ]; then
    echo "Exactly 2 arguments are required."
    return 1
  fi
  pattern=$1
  file=$2
  matched_lines=$(grep -c "$pattern" $file)
  if [ $matched_lines -eq 0 ]; then
    return 1
  fi
}

function process_setup_ubuntu {
  if [ -z "$(which git)" ]; then
    sudo --preserve-env apt install -y git
  fi
  # make this function Reentrant
  git checkout scripts/setup-ubuntu.sh

  # No need to re-install git.
  ensure_pattern_matched 'git ' scripts/setup-ubuntu.sh
  sed -i '/git \\/d' scripts/setup-ubuntu.sh
  # Do not install libunwind which can cause interruption when catching native exception.
  ensure_pattern_matched '\${SUDO} apt install -y libunwind-dev' scripts/setup-ubuntu.sh
  sed -i 's/${SUDO} apt install -y libunwind-dev//' scripts/setup-ubuntu.sh
  ensure_pattern_matched 'ccache' scripts/setup-ubuntu.sh
  sed -i '/ccache/a\    *thrift* \\' scripts/setup-ubuntu.sh
  sed -i '/ccache/a\    libiberty-dev \\' scripts/setup-ubuntu.sh
  sed -i '/ccache/a\    libxml2-dev \\' scripts/setup-ubuntu.sh
  sed -i '/ccache/a\    libkrb5-dev \\' scripts/setup-ubuntu.sh
  sed -i '/ccache/a\    libgsasl7-dev \\' scripts/setup-ubuntu.sh
  sed -i '/ccache/a\    libuuid1 \\' scripts/setup-ubuntu.sh
  sed -i '/ccache/a\    uuid-dev \\' scripts/setup-ubuntu.sh
  ensure_pattern_matched 'libgmock-dev' scripts/setup-ubuntu.sh
  sed -i '/libgmock-dev/d' scripts/setup-ubuntu.sh # resolved by ep/build-velox/build/velox_ep/CMake/resolve_dependency_modules/gtest.cmake
  ensure_pattern_matched 'function install_folly' scripts/setup-ubuntu.sh
  sed -i '/^function install_folly.*/i function install_protobuf {\n  wget_and_untar https://github.com/protocolbuffers/protobuf/releases/download/v21.4/protobuf-all-21.4.tar.gz protobuf\n  (\n    cd protobuf\n    ./configure  CXXFLAGS="-fPIC"  --prefix=/usr/local\n    make "-j$(nproc)"\n    sudo make install\n    sudo ldconfig\n  )\n}\n' scripts/setup-ubuntu.sh
  ensure_pattern_matched '  run_and_time install_folly' scripts/setup-ubuntu.sh
  sed -i '/^  run_and_time install_folly/a \ \ run_and_time install_protobuf' scripts/setup-ubuntu.sh
  # Required by lib hdfs.
  ensure_pattern_matched 'ccache ' scripts/setup-ubuntu.sh
  sed -i '/ccache /a\    yasm \\' scripts/setup-ubuntu.sh
  ensure_pattern_matched 'run_and_time install_conda' scripts/setup-ubuntu.sh
  sed -i '/run_and_time install_conda/d' scripts/setup-ubuntu.sh
  # Just depends on Gluten to install arrow libs since Gluten will apply some patches to Arrow source and uses different build options.
  ensure_pattern_matched 'run_and_time install_arrow' scripts/setup-ubuntu.sh
  sed -i '/run_and_time install_arrow/d' scripts/setup-ubuntu.sh
}

function process_setup_centos9 {
  # Allows other version of git already installed.
  if [ -z "$(which git)" ]; then
    dnf install -y -q --setopt=install_weak_deps=False git
  fi
  # make this function Reentrant
  git checkout scripts/setup-centos9.sh
  # No need to re-install git.

  ensure_pattern_matched 'dnf_install' scripts/setup-centos9.sh
  sed -i 's/dnf_install ninja-build cmake curl ccache gcc-toolset-12 git/dnf_install ninja-build cmake curl ccache gcc-toolset-12/' scripts/setup-centos9.sh
  sed -i '/^.*dnf_install autoconf/a\  dnf_install libxml2-devel libgsasl-devel libuuid-devel' scripts/setup-centos9.sh
  
  ensure_pattern_matched 'install_gflags' scripts/setup-centos9.sh
  sed -i '/^function install_gflags.*/i function install_openssl {\n  wget_and_untar https://github.com/openssl/openssl/archive/refs/tags/OpenSSL_1_1_1s.tar.gz openssl \n  cd openssl \n  ./config no-shared && make depend && make && sudo make install \n  cd ..\n}\n'     scripts/setup-centos9.sh

  ensure_pattern_matched 'install_fbthrift' scripts/setup-centos9.sh
  sed -i '/^  run_and_time install_fbthrift/a \  run_and_time install_openssl' scripts/setup-centos9.sh
  sed -i '/cd protobuf/{n;s/\.\/configure --prefix=\/usr/\.\/configure CXXFLAGS="-fPIC" --prefix=\/usr\/local/;}' scripts/setup-centos9.sh

  # Required by lib hdfs.
  ensure_pattern_matched 'dnf_install ninja-build' scripts/setup-centos9.sh
  sed -i '/^  dnf_install ninja-build/a\  dnf_install yasm\' scripts/setup-centos9.sh
}

function process_setup_alinux3 {
  sed -i "s/.*dnf_install epel-release/#&/" ${CURRENT_DIR}/setup-centos8.sh
  sed -i "s/.*run_and_time install_conda/#&/" ${CURRENT_DIR}/setup-centos8.sh
  sed -i "s/.*dnf config-manager --set-enabled powertools/#&/" ${CURRENT_DIR}/setup-centos8.sh
  sed -i "s/gcc-toolset-9 //" ${CURRENT_DIR}/setup-centos8.sh
  sed -i "s/.*source \/opt\/rh\/gcc-toolset-9\/enable/#&/" ${CURRENT_DIR}/setup-centos8.sh
  sed -i 's|^export CC=/opt/rh/gcc-toolset-9/root/bin/gcc|# &|' ${CURRENT_DIR}/setup-centos8.sh
  sed -i 's|^export CXX=/opt/rh/gcc-toolset-9/root/bin/g++|# &|' ${CURRENT_DIR}/setup-centos8.sh
  sed -i 's/python39 python39-devel python39-pip //g' ${CURRENT_DIR}/setup-centos8.sh
  sed -i "s/.*pip.* install/#&/" ${CURRENT_DIR}/setup-centos8.sh
}

function process_setup_tencentos32 {
  sed -i "s/.*dnf config-manager --set-enabled powertools/#&/" ${CURRENT_DIR}/setup-centos8.sh
}

echo "Preparing Velox source code..."

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

function apply_compilation_fixes {
  current_dir=$1
  velox_home=$2
  sudo cp ${current_dir}/modify_velox.patch ${velox_home}/
  sudo cp ${current_dir}/modify_arrow.patch ${velox_home}/CMake/resolve_dependency_modules/arrow/
  sudo cp ${current_dir}/modify_arrow_dataset_scan_option.patch ${velox_home}/CMake/resolve_dependency_modules/arrow/
  git add ${velox_home}/modify_velox.patch # to avoid the file from being deleted by git clean -dffx :/
  git add ${velox_home}/CMake/resolve_dependency_modules/arrow/modify_arrow.patch # to avoid the file from being deleted by git clean -dffx :/
  git add ${velox_home}/CMake/resolve_dependency_modules/arrow/modify_arrow_dataset_scan_option.patch # to avoid the file from being deleted by git clean -dffx :/
  cd ${velox_home}
  echo "Applying patch to Velox source code..."
  git apply modify_velox.patch
  if [ $? -ne 0 ]; then
    echo "Failed to apply compilation fixes to Velox: $?."
    exit 1
  fi
}

function setup_linux {
  local LINUX_DISTRIBUTION=$(. /etc/os-release && echo ${ID})
  local LINUX_VERSION_ID=$(. /etc/os-release && echo ${VERSION_ID})

  # apply patches
  sed -i 's/-mavx2 -mfma -mavx -mf16c -mlzcnt -std=c++17/-march=native -std=c++17 -mno-avx512f/g' scripts/setup-helper-functions.sh
  sed -i 's/SUDO="${SUDO:-""}"/SUDO="${SUDO:-"sudo --preserve-env"}"/g' scripts/setup-helper-functions.sh
  if [[ "$LINUX_DISTRIBUTION" == "ubuntu" || "$LINUX_DISTRIBUTION" == "debian" || "$LINUX_DISTRIBUTION" == "pop" ]]; then
    process_setup_ubuntu
  elif [[ "$LINUX_DISTRIBUTION" == "centos" ]]; then
    case "$LINUX_VERSION_ID" in
      9) process_setup_centos9 ;;
      8) ;;
      7) ;;
      *)
        echo "Unsupported centos version: $LINUX_VERSION_ID"
        exit 1
      ;;
    esac
  elif [[ "$LINUX_DISTRIBUTION" == "alinux" ]]; then
    case "${LINUX_VERSION_ID:0:1}" in
      2) ;;
      3) process_setup_alinux3 ;;
      *)
        echo "Unsupported alinux version: $LINUX_VERSION_ID"
        exit 1
      ;;
    esac
  elif [[ "$LINUX_DISTRIBUTION" == "tencentos" ]]; then
    case "$LINUX_VERSION_ID" in
      2.4) ;;
      3.2) process_setup_tencentos32 ;;
      *)
        echo "Unsupported tencentos version: $LINUX_VERSION_ID"
        exit 1
      ;;
    esac
  else
    echo "Unsupported linux distribution: $LINUX_DISTRIBUTION"
    exit 1
  fi
}

if [ $OS == 'Linux' ]; then
  setup_linux
elif [ $OS == 'Darwin' ]; then
  :
else
  echo "Unsupported kernel: $OS"
  exit 1
fi

apply_compilation_fixes $CURRENT_DIR $VELOX_SOURCE_DIR

echo "Finished getting Velox code"
