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
# deprecated, replaced by cmake command
set -exu

BUILD_TYPE=release
BUILD_VELOX_BACKEND=OFF
BUILD_TESTS=OFF
BUILD_EXAMPLES=OFF
BUILD_BENCHMARKS=OFF
ENABLE_JEMALLOC_STATS=OFF
ENABLE_QAT=OFF
ENABLE_HBM=OFF
ENABLE_GCS=OFF
ENABLE_S3=OFF
ENABLE_HDFS=OFF
ENABLE_ABFS=OFF
ENABLE_GPU=OFF
VELOX_HOME=
# set default number of threads as cpu cores minus 2
if [[ "$(uname)" == "Darwin" ]]; then
    physical_cpu_cores=$(sysctl -n hw.physicalcpu)
    ignore_cores=2
    if [ "$physical_cpu_cores" -gt "$ignore_cores" ]; then
        NPROC=${NPROC:-$(($physical_cpu_cores - $ignore_cores))}
    else
        NPROC=${NPROC:-$physical_cpu_cores}
    fi
else
    NPROC=${NPROC:-$(nproc --ignore=2)}
fi
echo "set default number of threads is ${NPROC}"

for arg in "$@"; do
  case $arg in
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
  --build_examples=*)
    BUILD_EXAMPLES=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --build_benchmarks=*)
    BUILD_BENCHMARKS=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --enable_jemalloc_stats=*)
    ENABLE_JEMALLOC_STATS=("${arg#*=}")
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
  --enable_gcs=*)
    ENABLE_GCS=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --enable_s3=*)
    ENABLE_S3=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --enable_abfs=*)
    ENABLE_ABFS=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --enable_hdfs=*)
    ENABLE_HDFS=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --enable_gpu=*)
    ENABLE_GPU=("${arg#*=}")
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

#gluten cpp will find velox lib from VELOX_HOME
if [ "$VELOX_HOME" == "" ]; then
  VELOX_HOME="$CURRENT_DIR/../ep/build-velox/build/velox_ep"
fi

echo "Building gluten cpp part..."
echo "CMAKE Arguments:"
echo "VELOX_HOME=${VELOX_HOME}"
echo "BUILD_TYPE=${BUILD_TYPE}"
echo "BUILD_VELOX_BACKEND=${BUILD_VELOX_BACKEND}"
echo "BUILD_TESTS=${BUILD_TESTS}"
echo "BUILD_EXAMPLES=${BUILD_EXAMPLES}"
echo "BUILD_BENCHMARKS=${BUILD_BENCHMARKS}"
echo "ENABLE_JEMALLOC_STATS=${ENABLE_JEMALLOC_STATS}"
echo "ENABLE_HBM=${ENABLE_HBM}"
echo "ENABLE_GCS=${ENABLE_GCS}"
echo "ENABLE_S3=${ENABLE_S3}"
echo "ENABLE_HDFS=${ENABLE_HDFS}"
echo "ENABLE_ABFS=${ENABLE_ABFS}"
echo "ENABLE_GPU=${ENABLE_GPU}"

if [ -d build ]; then
  rm -r build
fi
mkdir build
cd build
cmake .. \
  -DBUILD_TESTS=${BUILD_TESTS} \
  -DBUILD_EXAMPLES=${BUILD_EXAMPLES} \
  -DENABLE_JEMALLOC_STATS=${ENABLE_JEMALLOC_STATS} \
  -DBUILD_VELOX_BACKEND=${BUILD_VELOX_BACKEND} \
  -DVELOX_HOME=${VELOX_HOME} \
  -DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
  -DBUILD_BENCHMARKS=${BUILD_BENCHMARKS} \
  -DENABLE_QAT=${ENABLE_QAT} \
  -DENABLE_HBM=${ENABLE_HBM} \
  -DENABLE_GCS=${ENABLE_GCS} \
  -DENABLE_S3=${ENABLE_S3} \
  -DENABLE_HDFS=${ENABLE_HDFS} \
  -DENABLE_ABFS=${ENABLE_ABFS} \
  -DENABLE_GPU=${ENABLE_GPU}
make -j$NPROC
