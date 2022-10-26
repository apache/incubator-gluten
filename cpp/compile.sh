#!/usr/bin/env bash

set -eu
set -x

BUILD_CPP=${1:-ON}
BUILD_TESTS=${2:-OFF}
BUILD_ARROW=${3:-ON}
STATIC_ARROW=${4:-OFF}
BUILD_PROTOBUF=${5:-ON}
ARROW_ROOT=${6:-/usr/local}
ARROW_BFS_INSTALL_DIR=${7}
BUILD_JEMALLOC=${8:-ON}
BUILD_GAZELLE_CPP=${9:-OFF}
BUILD_VELOX=${10:-OFF}
VELOX_HOME=${11:-/root/velox}
VELOX_BUILD_TYPE=${12:-release}
DEBUG_BUILD=${13:-OFF}
BUILD_BENCHMARKS=${14:-OFF}
BACKEND_TYPE=${15:-velox}

if [ "$BUILD_CPP" == "ON" ]; then
  NPROC=$(nproc --ignore=2)

  CURRENT_DIR=$(
    cd "$(dirname "$BASH_SOURCE")"
    pwd
  )
  cd "${CURRENT_DIR}"

  if [ -d build ]; then
    rm -r build
  fi
  mkdir build
  cd build
  cmake .. \
    -DBUILD_TESTS=${BUILD_TESTS} \
    -DBUILD_ARROW=${BUILD_ARROW} \
    -DSTATIC_ARROW=${STATIC_ARROW} \
    -DBUILD_PROTOBUF=${BUILD_PROTOBUF} \
    -DARROW_ROOT=${ARROW_ROOT} \
    -DARROW_BFS_INSTALL_DIR=${ARROW_BFS_INSTALL_DIR} \
    -DBUILD_JEMALLOC=${BUILD_JEMALLOC} \
    -DBUILD_GAZELLE_CPP=${BUILD_GAZELLE_CPP} \
    -DBUILD_VELOX=${BUILD_VELOX} \
    -DVELOX_HOME=${VELOX_HOME} \
    -DVELOX_BUILD_TYPE=${VELOX_BUILD_TYPE} \
    -DCMAKE_BUILD_TYPE=$(if [ "$DEBUG_BUILD" == 'ON' ]; then echo 'Debug'; else echo 'Release'; fi) \
    -DDEBUG=${DEBUG_BUILD} \
    -DBUILD_BENCHMARKS=${BUILD_BENCHMARKS} \
    -DBACKEND_TYPE=${BACKEND_TYPE}
  make -j$NPROC
fi
