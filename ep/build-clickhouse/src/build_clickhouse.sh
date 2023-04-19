#!/bin/bash

export GLUTEN_SOURCE=$(builtin cd $(dirname $0)/../../..; pwd)
export CH_SOURCE_DIR=${GLUTEN_SOURCE}/cpp-ch/ClickHouse

for arg in "$@"
do
    case $arg in
        -t=*|--src=*)
        GLUTEN_SOURCE=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        -s=*|--ch=*)
        CH_SOURCE_DIR=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        *)
        OTHER_ARGUMENTS+=("$1")
        shift # Remove generic argument from processing
        ;;
    esac
done

echo ${GLUTEN_SOURCE}

cmake -G Ninja -S ${GLUTEN_SOURCE}/cpp-ch -B ${GLUTEN_SOURCE}/cpp-ch/build_ch -DCH_SOURCE_DIR=${CH_SOURCE_DIR} "-DCMAKE_C_COMPILER=$(command -v clang-15)" "-DCMAKE_CXX_COMPILER=$(command -v clang++-15)" "-DCMAKE_BUILD_TYPE=RelWithDebInfo"
cmake --build ${GLUTEN_SOURCE}/cpp-ch/build_ch --target build_ch
