#!/bin/bash
# deprecated, replaced by cmake command
set -exu

BUILD_TYPE=release
BUILD_VELOX_BACKEND=OFF
BUILD_TESTS=OFF
BUILD_BENCHMARKS=OFF
BUILD_JEMALLOC=OFF
BUILD_PROTOBUF=OFF
ENABLE_QAT=OFF
ENABLE_HBM=OFF
ENABLE_S3=OFF
ENABLE_HDFS=OFF
NPROC=$(nproc --ignore=2)
ARROW_ROOT=
VELOX_HOME=

for arg in "$@"; do
  case $arg in
  --arrow_root=*)
    ARROW_ROOT=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --velox_home=*)
    VELOX_HOME=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --build_type=*)
    BUILD_TYPE=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --build_velox_backend=*)
    BUILD_VELOX_BACKEND=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --build_tests=*)
    BUILD_TESTS=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --build_benchmarks=*)
    BUILD_BENCHMARKS=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --build_jemalloc=*)
    BUILD_JEMALLOC=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --enable_qat=*)
    ENABLE_QAT=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --enable_hbm=*)
    ENABLE_HBM=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --build_protobuf=*)
    BUILD_PROTOBUF=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --enable_s3=*)
    ENABLE_S3=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --enable_hdfs=*)
    ENABLE_HDFS=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  *)
    OTHER_ARGUMENTS+=("$1")
    shift # Remove generic argument from processing
    ;;
  esac
done

CURRENT_DIR=$(
  cd "$(dirname "$BASH_SOURCE")"
  pwd
)
#gluten cpp will find arrow lib from ARROW_ROOT
if [ "$ARROW_ROOT" == "" ]; then
  ARROW_ROOT="$CURRENT_DIR/../ep/build-arrow/build/arrow_install"
fi

#gluten cpp will find velox lib from VELOX_HOME
if [ "$VELOX_HOME" == "" ]; then
  VELOX_HOME="$CURRENT_DIR/../ep/build-velox/build/velox_ep"
fi

echo "Building gluten cpp part..."
echo "CMAKE Arguments:"
echo "ARROW_ROOT=${ARROW_ROOT}"
echo "VELOX_HOME=${VELOX_HOME}"
echo "BUILD_TYPE=${BUILD_TYPE}"
echo "BUILD_VELOX_BACKEND=${BUILD_VELOX_BACKEND}"
echo "BUILD_TESTS=${BUILD_TESTS}"
echo "BUILD_BENCHMARKS=${BUILD_BENCHMARKS}"
echo "BUILD_JEMALLOC=${BUILD_JEMALLOC}"
echo "ENABLE_HBM=${ENABLE_HBM}"
echo "BUILD_PROTOBUF=${BUILD_PROTOBUF}"
echo "ENABLE_S3=${ENABLE_S3}"
echo "ENABLE_HDFS=${ENABLE_HDFS}"

if [ -d build ]; then
  rm -r build
fi
mkdir build
cd build
cmake .. \
  -DBUILD_TESTS=${BUILD_TESTS} \
  -DARROW_ROOT=${ARROW_ROOT} \
  -DBUILD_JEMALLOC=${BUILD_JEMALLOC} \
  -DBUILD_VELOX_BACKEND=${BUILD_VELOX_BACKEND} \
  -DVELOX_HOME=${VELOX_HOME} \
  -DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
  -DBUILD_BENCHMARKS=${BUILD_BENCHMARKS} \
  -DBUILD_PROTOBUF=${BUILD_PROTOBUF} \
  -DENABLE_QAT=${ENABLE_QAT} \
  -DENABLE_HBM=${ENABLE_HBM} \
  -DENABLE_S3=${ENABLE_S3} \
  -DENABLE_HDFS=${ENABLE_HDFS}
make -j$NPROC
