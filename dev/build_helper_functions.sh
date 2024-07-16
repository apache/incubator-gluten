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

function get_cxx_flags {
  local CPU_ARCH=$1

  local OS
  OS=$(uname)
  local MACHINE
  MACHINE=$(uname -m)
  ADDITIONAL_FLAGS=""

  if [[ -z "$CPU_ARCH" ]] || [[ $CPU_ARCH == "unknown" ]]; then
    if [ "$OS" = "Darwin" ]; then

      if [ "$MACHINE" = "x86_64" ]; then
        local CPU_CAPABILITIES
        CPU_CAPABILITIES=$(sysctl -a | grep machdep.cpu.features | awk '{print tolower($0)}')

        if [[ $CPU_CAPABILITIES =~ "avx" ]]; then
          CPU_ARCH="avx"
        else
          CPU_ARCH="sse"
        fi

      elif [[ $(sysctl -a | grep machdep.cpu.brand_string) =~ "Apple" ]]; then
        # Apple silicon.
        CPU_ARCH="arm64"
      fi

    # On MacOs prevent the flood of translation visibility settings warnings.
    ADDITIONAL_FLAGS="-fvisibility=hidden -fvisibility-inlines-hidden"
    else [ "$OS" = "Linux" ];

      local CPU_CAPABILITIES
      CPU_CAPABILITIES=$(cat /proc/cpuinfo | grep flags | head -n 1| awk '{print tolower($0)}')

      if [[ "$CPU_CAPABILITIES" =~ "avx" ]]; then
            CPU_ARCH="avx"
      elif [[ "$CPU_CAPABILITIES" =~ "sse" ]]; then
            CPU_ARCH="sse"
      elif [ "$MACHINE" = "aarch64" ]; then
            CPU_ARCH="aarch64"
      fi
    fi
  fi

  case $CPU_ARCH in

    "arm64")
      echo -n "-mcpu=apple-m1+crc -std=c++17 -fvisibility=hidden $ADDITIONAL_FLAGS"
    ;;

    "avx")
      echo -n "-mavx2 -mfma -mavx -mf16c -mlzcnt -std=c++17 -mbmi2 $ADDITIONAL_FLAGS"
    ;;

    "sse")
      echo -n "-msse4.2 -std=c++17 $ADDITIONAL_FLAGS"
    ;;

    "aarch64")
      echo -n "-mcpu=neoverse-n1 -std=c++17 $ADDITIONAL_FLAGS"
    ;;
  *)
    echo -n "Architecture not supported!"
  esac

}

function wget_and_untar {
  local URL=$1
  local DIR=$2
  mkdir -p "${DIR}"
  pushd "${DIR}"
  curl -L "${URL}" > $2.tar.gz
  tar -xz --strip-components=1 -f $2.tar.gz
  popd
}

function cmake_install {
  local NAME=$(basename "$(pwd)")
  local BINARY_DIR=_build
  SUDO="${SUDO:-""}"
  if [ -d "${BINARY_DIR}" ] && prompt "Do you want to rebuild ${NAME}?"; then
    ${SUDO} rm -rf "${BINARY_DIR}"
  fi
  mkdir -p "${BINARY_DIR}"
  CPU_TARGET="${CPU_TARGET:-unknown}"
  COMPILER_FLAGS=$(get_cxx_flags $CPU_TARGET)

  # CMAKE_POSITION_INDEPENDENT_CODE is required so that Velox can be built into dynamic libraries \
  cmake -Wno-dev -B"${BINARY_DIR}" \
    -GNinja \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    -DCMAKE_CXX_STANDARD=17 \
    "${INSTALL_PREFIX+-DCMAKE_PREFIX_PATH=}${INSTALL_PREFIX-}" \
    "${INSTALL_PREFIX+-DCMAKE_INSTALL_PREFIX=}${INSTALL_PREFIX-}" \
    -DCMAKE_CXX_FLAGS="$COMPILER_FLAGS" \
    -DBUILD_TESTING=OFF \
    "$@"

  cmake --build "${BINARY_DIR}"
  ${SUDO} cmake --install "${BINARY_DIR}"
}

function setup_macos {
  if [ $ARCH == 'x86_64' ]; then
    ./scripts/setup-macos.sh
  elif [ $ARCH == 'arm64' ]; then
    CPU_TARGET="arm64" ./scripts/setup-macos.sh
  else
    echo "Unknown arch: $ARCH"
  fi
}

function setup_linux {
  local LINUX_DISTRIBUTION=$(. /etc/os-release && echo ${ID})
  local LINUX_VERSION_ID=$(. /etc/os-release && echo ${VERSION_ID})

  if [[ "$LINUX_DISTRIBUTION" == "ubuntu" || "$LINUX_DISTRIBUTION" == "debian" || "$LINUX_DISTRIBUTION" == "pop" ]]; then
    scripts/setup-ubuntu.sh
  elif [[ "$LINUX_DISTRIBUTION" == "centos" ]]; then
    case "$LINUX_VERSION_ID" in
    8) scripts/setup-centos8.sh ;;
    7)
      scripts/setup-centos7.sh
      set +u
      export PKG_CONFIG_PATH=/usr/local/lib64/pkgconfig:/usr/local/lib/pkgconfig:/usr/lib64/pkgconfig:/usr/lib/pkgconfig:$PKG_CONFIG_PATH
      source /opt/rh/devtoolset-9/enable
      set -u
      ;;
    *)
      echo "Unsupported centos version: $LINUX_VERSION_ID"
      exit 1
      ;;
    esac
  elif [[ "$LINUX_DISTRIBUTION" == "alinux" ]]; then
    case "${LINUX_VERSION_ID:0:1}" in
    2)
      scripts/setup-centos7.sh
      set +u
      export PKG_CONFIG_PATH=/usr/local/lib64/pkgconfig:/usr/local/lib/pkgconfig:/usr/lib64/pkgconfig:/usr/lib/pkgconfig:$PKG_CONFIG_PATH
      source /opt/rh/devtoolset-9/enable
      set -u
      ;;
    3) scripts/setup-centos8.sh ;;
    *)
      echo "Unsupported alinux version: $LINUX_VERSION_ID"
      exit 1
      ;;
    esac
  elif [[ "$LINUX_DISTRIBUTION" == "tencentos" ]]; then
    case "$LINUX_VERSION_ID" in
    2.4)
        scripts/setup-centos7.sh
        set +u
        export PKG_CONFIG_PATH=/usr/local/lib64/pkgconfig:/usr/local/lib/pkgconfig:/usr/lib64/pkgconfig:/usr/lib/pkgconfig:$PKG_CONFIG_PATH
        source /opt/rh/devtoolset-9/enable
        set -u
        ;;
    3.2) scripts/setup-centos8.sh ;;
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
