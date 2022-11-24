#!/bin/bash

set -exu

#setting gluten root path
GLUTEN_DIR=/opt/gluten
BUILD_TYPE=release
NPROC=$(nproc --ignore=2)

for arg in "$@"
do
    case $arg in
        --gluten_dir=*)
        GLUTEN_DIR=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --build_dir=*)
        BUILD_DIR=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --build_type=*)
        BUILD_TYPE=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
	    *)
	    OTHER_ARGUMENTS+=("$1")
        shift # Remove generic argument from processing
        ;;
    esac
done

BUILD_DIR="$GLUTEN_DIR/ep/build-arrow/build"
ARROW_SOURCE_DIR="${BUILD_DIR}/arrow_ep"
ARROW_INSTALL_DIR="${BUILD_DIR}/arrow_install"
echo "Building Arrow from Source for Gazelle..."
echo "CMAKE Arguments:"
echo "GLUTEN_DIR=${GLUTEN_DIR}"
echo "BUILD_DIR=${BUILD_DIR}"
echo "BUILD_TYPE=${BUILD_TYPE}"

if [ -d $ARROW_INSTALL_DIR ]; then
    rm -rf $ARROW_INSTALL_DIR
fi

mkdir -p $ARROW_INSTALL_DIR

pushd $ARROW_SOURCE_DIR
mkdir -p java/c/build
pushd java/c/build
cmake ..
cmake --build .
popd

cmake -DARROW_BUILD_STATIC=OFF \
        -DARROW_BUILD_SHARED=ON \
        -DARROW_COMPUTE=ON \
        -DARROW_SUBSTRAIT=ON \
        -DARROW_S3=ON \
        -DARROW_PARQUET=ON \
        -DARROW_ORC=OFF \
        -DARROW_HDFS=ON \
        -DARROW_BOOST_USE_SHARED=OFF \
        -DARROW_JNI=ON \
        -DARROW_DATASET=ON \
        -DARROW_WITH_PROTOBUF=ON \
        -DARROW_PROTOBUF_USE_SHARED=OFF \
        -DARROW_WITH_SNAPPY=ON \
        -DARROW_WITH_LZ4=ON \
        -DARROW_WITH_RE2=ON \
        -DARROW_FILESYSTEM=ON \
        -DARROW_JEMALLOC=ON \
        -DARROW_SIMD_LEVEL=AVX2 \
        -DARROW_RUNTIME_SIMD_LEVEL=MAX \
        -DARROW_DEPENDENCY_SOURCE=BUNDLED \
        -Dre2_SOURCE=BUNDLED \
        -DProtobuf_SOURCE=BUNDLED \
        -DCMAKE_INSTALL_PREFIX=$ARROW_INSTALL_DIR \
        -DCMAKE_INSTALL_LIBDIR=lib \
        cpp

make -j$NPROC

make install

cd java
mvn clean install -P arrow-jni -pl dataset,c -am -Darrow.cpp.build.dir=$ARROW_INSTALL_DIR/lib -DskipTests -Dcheckstyle.skip -Dmaven.gitcommitid.skip=true