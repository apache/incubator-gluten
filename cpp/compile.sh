#!/bin/bash

set -exu

GLUTEN_DIR=/home/gluten
BUILD_TYPE=release
BUILD_GAZELLE_CPP_BACKEND=OFF
BUILD_VELOX_BACKEND=ON
BUILD_TESTS=OFF
BUILD_BENCHMARKS=OFF
BUILD_JEMALLOC=ON
ENABLE_HBM=OFF
BUILD_PROTOBUF=OFF
ENABLE_S3=OFF
ENABLE_HDFS=OFF
NPROC=4

for arg in "$@"
do
    case $arg in
        --gluten_dir=*)
        GLUTEN_DIR=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
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
        --build_gazelle_cpp_backend=*)
        BUILD_GAZELLE_CPP_BACKEND=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --build_velox_backend=*)
        BUILD_VELOX_BACKEND=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --build_test=*)
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

#gluten cpp will find arrow lib from ARROW_ROOT
ARROW_ROOT="$GLUTEN_DIR/ep/build-arrow/build/arrow_install"
#gluten cpp will find velox lib from VELOX_HOME
VELOX_HOME="$GLUTEN_DIR/ep/build-velox/build/velox_ep"
echo "Building gluten cpp part..."
echo "CMAKE Arguments:"
echo "GLUTEN_DIR=${GLUTEN_DIR}"
echo "ARROW_ROOT=${ARROW_ROOT}"
echo "VELOX_HOME=${VELOX_HOME}"
echo "BUILD_TYPE=${BUILD_TYPE}"
echo "BUILD_GAZELLE_CPP_BACKEND=${BUILD_GAZELLE_CPP_BACKEND}"
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
  -DBUILD_GAZELLE_CPP_BACKEND=${BUILD_GAZELLE_CPP_BACKEND} \
  -DBUILD_VELOX_BACKEND=${BUILD_VELOX_BACKEND} \
  -DVELOX_HOME=${VELOX_HOME} \
  -DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
  -DBUILD_BENCHMARKS=${BUILD_BENCHMARKS} \
  -DENABLE_HBM=${ENABLE_HBM} \
  -DBUILD_PROTOBUF=${BUILD_PROTOBUF} \
  -DVELOX_ENABLE_S3=${ENABLE_S3} \
  -DVELOX_ENABLE_HDFS=${ENABLE_HDFS}
make -j$NPROC
