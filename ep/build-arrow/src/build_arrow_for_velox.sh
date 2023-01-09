#!/bin/bash

set -exu

BUILD_TESTS=OFF
BUILD_TYPE=release
NPROC=$(nproc --ignore=2)
TARGET_BUILD_COMMIT=""
ARROW_REPO=https://github.com/oap-project/arrow.git
ARROW_BRANCH=backend_velox_main
ARROW_HOME=

for arg in "$@"
do
    case $arg in
        --build_test=*)
        BUILD_TESTS=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --build_type=*)
        BUILD_TYPE=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --arrow_home=*)
        ARROW_HOME=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        *)
        OTHER_ARGUMENTS+=("$1")
        shift # Remove generic argument from processing
        ;;
    esac
done

CURRENT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
if [ "$ARROW_HOME" == "" ]; then
  ARROW_HOME="$CURRENT_DIR/../build/"
fi

ARROW_SOURCE_DIR="${ARROW_HOME}/arrow_ep"
ARROW_INSTALL_DIR="${ARROW_HOME}/arrow_install"

echo "Building Arrow from Source for Velox..."
echo "CMAKE Arguments:"
echo "BUILD_TESTS=${BUILD_TESTS}"
echo "BUILD_TYPE=${BUILD_TYPE}"
echo "ARROW_HOME=${ARROW_HOME}"

if [ -d $ARROW_INSTALL_DIR ]; then
    rm -rf $ARROW_INSTALL_DIR
fi

mkdir -p $ARROW_INSTALL_DIR

WITH_JSON=OFF
if [ $BUILD_TESTS == ON ]; then
  WITH_JSON=ON
fi
pushd $ARROW_SOURCE_DIR
TARGET_BUILD_COMMIT=$(git rev-parse --verify HEAD)
mkdir -p java/build
pushd java/build
cmake \
    -DCMAKE_INSTALL_PREFIX=$ARROW_INSTALL_DIR/lib \
    ..
cmake --build . --target install
popd

mkdir -p cpp/build
pushd cpp/build
cmake -G Ninja \
        -DARROW_BUILD_STATIC=OFF \
        -DARROW_COMPUTE=ON \
        -DARROW_WITH_RE2=ON \
        -DARROW_FILESYSTEM=ON \
        -DARROW_WITH_LZ4=ON \
        -DARROW_WITH_SNAPPY=ON \
        -DARROW_WITH_ZLIB=ON \
        -DARROW_JSON=$WITH_JSON \
        -DARROW_PARQUET=ON \
        -DARROW_WITH_ZSTD=ON \
        -DARROW_BUILD_SHARED=ON \
        -DARROW_BOOST_USE_SHARED=OFF \
        -DARROW_JNI=ON \
        -DARROW_JEMALLOC=ON \
        -DARROW_SIMD_LEVEL=AVX2 \
        -DARROW_RUNTIME_SIMD_LEVEL=MAX \
        -DARROW_DEPENDENCY_SOURCE=BUNDLED \
        -Dre2_SOURCE=AUTO \
        -DCMAKE_INSTALL_PREFIX=$ARROW_INSTALL_DIR \
        -DCMAKE_INSTALL_LIBDIR=lib \
        -DCMAKE_BUILD_TYPE=$BUILD_TYPE \
        ..
cmake --build . --target install
popd

cd java
mvn clean install -P arrow-jni -pl c -am -Darrow.cpp.build.dir=$ARROW_INSTALL_DIR/lib -DskipTests -Dcheckstyle.skip \
    -Darrow.c.jni.dist.dir=$ARROW_INSTALL_DIR/lib -Dmaven.gitcommitid.skip=true

echo "Successfully built Arrow from Source !!!"
echo $TARGET_BUILD_COMMIT > "${ARROW_HOME}/arrow-commit.cache"
