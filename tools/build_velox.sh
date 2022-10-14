#!/bin/bash

set -eu

NPROC=$(nproc)

BUILD_VELOX_FROM_SOURCE=OFF
COMPILE_VELOX=OFF
VELOX_HOME=${3:-/root/velox}
ENABLE_EP_CACHE=OFF

VELOX_REPO=https://github.com/oap-project/velox.git
VELOX_BRANCH=main

for arg in "$@"
do
    case $arg in
        --build_velox_from_source=*)
        BUILD_VELOX_FROM_SOURCE=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --compile_velox=*)
        COMPILE_VELOX=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --enable_ep_cache=*)
        ENABLE_EP_CACHE=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
	*)
	OTHER_ARGUMENTS+=("$1")
        shift # Remove generic argument from processing
        ;;
    esac
done

function process_script {
    # make this function Reentrantly
    git checkout scripts/setup-ubuntu.sh
    sed -i '/libprotobuf-dev/d' scripts/setup-ubuntu.sh
    sed -i '/protobuf-compiler/d' scripts/setup-ubuntu.sh
    sed -i '/^sudo apt install/a\  libiberty-dev \\' scripts/setup-ubuntu.sh
    sed -i 's/^  liblzo2-dev.*/  liblzo2-dev \\/g' scripts/setup-ubuntu.sh
    sed -i 's/^  ninja -C "${BINARY_DIR}" install/  sudo ninja -C "${BINARY_DIR}" install/g' scripts/setup-helper-functions.sh
    sed -i '/^function install_folly.*/i function install_pb {\n  github_checkout protocolbuffers/protobuf v3.13.0\n  git submodule update --init --recursive\n  ./autogen.sh\n  ./configure CFLAGS=-fPIC CXXFLAGS=-fPIC\n  make -j$(nproc)\n  make check\n  sudo make install\n sudo ldconfig\n}\n' scripts/setup-ubuntu.sh
    sed -i '/^  run_and_time install_folly/i \ \ run_and_time install_pb' scripts/setup-ubuntu.sh
    sed -i 's/-mavx2 -mfma -mavx -mf16c -mlzcnt -std=c++17/-march=native -std=c++17 -mno-avx512f/g' scripts/setup-helper-functions.sh
}

function compile {
    scripts/setup-ubuntu.sh
    make release EXTRA_CMAKE_FLAGS=" -DVELOX_ENABLE_PARQUET=ON -DVELOX_ENABLE_ARROW=ON"
}

echo "Velox Installation"

CURRENT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
echo $CURRENT_DIR

cd ${CURRENT_DIR}


if [ $BUILD_VELOX_FROM_SOURCE == "ON" ]; then

    TARGET_BUILD_COMMIT="$(git ls-remote $VELOX_REPO $VELOX_BRANCH | awk '{print $1;}')"
    if [ $ENABLE_EP_CACHE == "ON" ]; then
        if [ -e ${CURRENT_DIR}/velox-commit.cache ]; then
            LAST_BUILT_COMMIT="$(cat ${CURRENT_DIR}/velox-commit.cache)"
            if [ -n $LAST_BUILT_COMMIT ]; then
                if [ -z "$TARGET_BUILD_COMMIT" ]
                then
                  echo "Unable to parse Velox commit: $TARGET_BUILD_COMMIT."
                  exit 1
                fi

                if [ "$TARGET_BUILD_COMMIT" = "$LAST_BUILT_COMMIT" ]; then
                    echo "Velox build of commit $TARGET_BUILD_COMMIT was cached, skipping build..."
                    exit 0
                else
                    echo "Found cached commit $LAST_BUILT_COMMIT for Velox which is different with target commit $TARGET_BUILD_COMMIT, creating brand-new build..."
                fi
            fi
        fi
    fi

    if [ -d build/velox_ep ]; then
        rm -r build/velox_ep
    fi

    if [ -d build/velox_install ]; then
        rm -r build/velox_install
    fi

    if [ -e ${CURRENT_DIR}/velox-commit.cache ]; then
        rm -f ${CURRENT_DIR}/velox-commit.cache
    fi

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
    git clone $VELOX_REPO -b $VELOX_BRANCH $VELOX_SOURCE_DIR
    pushd $VELOX_SOURCE_DIR
    #sync submodules
    git submodule sync --recursive
    git submodule update --init --recursive

    process_script
    compile
    echo "Finish to build Velox from Source !!!"
    echo $TARGET_BUILD_COMMIT > "${CURRENT_DIR}/velox-commit.cache"
else
    VELOX_SOURCE_DIR=${VELOX_HOME}
    if [ $COMPILE_VELOX == "ON" ]; then
        echo "VELOX_SOURCE_DIR=${VELOX_SOURCE_DIR}"
        pushd $VELOX_SOURCE_DIR
        process_script
        compile
    fi
    echo "Use existing Velox at ${VELOX_SOURCE_DIR}."
fi

