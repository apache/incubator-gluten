#!/bin/bash

set -eu

NPROC=$(nproc)

TESTS=OFF
BUILD_ARROW=OFF
STATIC_ARROW=OFF
ARROW_ROOT=/usr/local

for arg in "$@"
do
    case $arg in
        -t=*|--tests=*)
        TESTS=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        -a=*|--build_arrow=*)
        BUILD_ARROW=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        -s=*|--static_arrow=*)
        STATIC_ARROW=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        -ar=*|--arrow_root=*)
        ARROW_ROOT=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        *)
        OTHER_ARGUMENTS+=("$1")
        shift # Remove generic argument from processing
        ;;
    esac
done

echo "CMAKE Arguments:"
echo "TESTS=${TESTS}"
echo "BUILD_ARROW=${BUILD_ARROW}"
echo "STATIC_ARROW=${STATIC_ARROW}"
echo "ARROW_ROOT=${ARROW_ROOT}"

CURRENT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
echo $CURRENT_DIR

cd ${CURRENT_DIR}
if [ -d build ]; then
    rm -r build
fi

if [ $BUILD_ARROW == "ON" ]; then
echo "Building Arrow from Source ..."
mkdir build
cd build
ARROW_PREFIX="${CURRENT_DIR}/build" # Use build directory as ARROW_PREFIX
ARROW_SOURCE_DIR="${ARROW_PREFIX}/arrow_ep"
ARROW_INSTALL_DIR="${ARROW_PREFIX}/arrow_install"

echo "ARROW_PREFIX=${ARROW_PREFIX}"
echo "ARROW_SOURCE_DIR=${ARROW_SOURCE_DIR}"
echo "ARROW_INSTALL_DIR=${ARROW_INSTALL_DIR}"
mkdir -p $ARROW_SOURCE_DIR
mkdir -p $ARROW_INSTALL_DIR
git clone https://github.com/oap-project/arrow.git  --branch arrow-4.0.0-oap $ARROW_SOURCE_DIR
pushd $ARROW_SOURCE_DIR

cmake ./cpp \
	-DARROW_BUILD_STATIC=OFF -DARROW_BUILD_SHARED=ON -DARROW_COMPUTE=ON \
        -DARROW_S3=ON \
        -DARROW_GANDIVA_JAVA=ON \
        -DARROW_GANDIVA=ON \
        -DARROW_PARQUET=ON \
        -DARROW_ORC=ON \
        -DARROW_HDFS=ON \
        -DARROW_BOOST_USE_SHARED=OFF \
        -DARROW_JNI=ON \
        -DARROW_DATASET=ON \
        -DARROW_WITH_PROTOBUF=ON \
        -DARROW_WITH_SNAPPY=ON \
        -DARROW_WITH_LZ4=ON \
        -DARROW_WITH_ZSTD=OFF \
        -DARROW_WITH_BROTLI=OFF \
        -DARROW_WITH_ZLIB=OFF \
        -DARROW_WITH_FASTPFOR=ON \
        -DARROW_FILESYSTEM=ON \
        -DARROW_JSON=ON \
        -DARROW_CSV=ON \
        -DARROW_FLIGHT=OFF \
        -DARROW_JEMALLOC=ON \
        -DARROW_SIMD_LEVEL=AVX2 \
        -DARROW_RUNTIME_SIMD_LEVEL=MAX \
        -DARROW_DEPENDENCY_SOURCE=BUNDLED \
        -DCMAKE_INSTALL_PREFIX=${ARROW_INSTALL_DIR} \
        -DCMAKE_INSTALL_LIBDIR=lib

make -j$NPROC
make install

cd java        
mvn clean install -P arrow-jni -am -Darrow.cpp.build.dir=${ARROW_INSTALL_DIR}/lib -DskipTests -Dcheckstyle.skip
popd
echo "Finish to build Arrow from Source !!!"
else
echo "Use ARROW_ROOT as Arrow Library Path"
echo "ARROW_ROOT=${ARROW_ROOT}"
fi
