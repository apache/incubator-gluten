#!/bin/bash

set -eu

NPROC=$(nproc)

BUILD_VELOX_FROM_SOURCE=OFF

for arg in "$@"
do
    case $arg in
        -v=*|--build_velox_from_source=*)
        BUILD_VELOX_FROM_SOURCE=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
	*)
	OTHER_ARGUMENTS+=("$1")
        shift # Remove generic argument from processing
        ;;
    esac
done


echo "Velox Installation"

CURRENT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
echo $CURRENT_DIR

cd ${CURRENT_DIR}
if [ -d build/velox_ep ]; then
    rm -r build/velox_ep
fi

if [ -d build/velox_install ]; then
    rm -r build/velox_install
fi

if [ $BUILD_VELOX_FROM_SOURCE == "ON" ]; then
    echo "Building Velox from Source ..."
    mkdir -p build
    cd build
    VELOX_PREFIX="${CURRENT_DIR}/build" # Use build directory as VELOX_PREFIX
    VELOX_SOURCE_DIR="${VELOX_PREFIX}/velox_ep"
    VELOX_INSTALL_DIR="${VELOX_PREFIX}/velox_install"

    echo "VELOX_PREFIX=${VELOX_PREFIX}"
    echo "VELOX_SOURCE_DIR=${VELOX_SOURCE_DIR}"
    echo "VELOX_INSTALL_DIR=${VELOX_INSTALL_DIR}"
    mkdir -p $VELOX_SOURCE_DIR
    mkdir -p $VELOX_INSTALL_DIR
    git clone https://github.com/oap-project/velox.git -b main $VELOX_SOURCE_DIR
    pushd $VELOX_SOURCE_DIR

    #sync submodules
    git submodule sync --recursive
    git submodule update --init --recursive

    scripts/setup-ubuntu.sh
    make release
    echo "Finish to build Velox from Source !!!"
else
    echo "Use existing Velox."
fi
