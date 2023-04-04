#!/bin/bash

export CLICKHOUSE_SRC=''
export BUILD_DIR=''

for arg in "$@"
do
    case $arg in
        -t=*|--src=*)
        CLICKHOUSE_SRC=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        -a=*|--build_dir=*)
        BUILD_DIR=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        *)
        OTHER_ARGUMENTS+=("$1")
        shift # Remove generic argument from processing
        ;;
    esac
done

mkdir -p ${BUILD_DIR}
cd ${BUILD_DIR}
cmake -G Ninja  "-DCMAKE_C_COMPILER=$(command -v clang-12)" "-DCMAKE_CXX_COMPILER=$(command -v clang++-12)" "-DCMAKE_BUILD_TYPE=Release" "-DENABLE_PROTOBUF=1" "-DWERROR=OFF" "-DENABLE_JEMALLOC=0" ${CLICKHOUSE_SRC}

ninja ch
