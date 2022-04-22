#!/bin/bash

set -eu

NPROC=$(nproc)
rm -rf arrow
rm -rf /tmp/arrow_install.7
git clone https://github.com/oap-project/arrow.git -b arrow-7.0.0-oap
cd arrow
cmake -DARROW_BUILD_STATIC=OFF \
        -DARROW_BUILD_SHARED=ON \
        -DARROW_COMPUTE=ON \
        -DARROW_ENGINE=ON \
        -DARROW_S3=ON \
        -DARROW_GANDIVA_JAVA=ON \
        -DARROW_GANDIVA=ON \
        -DARROW_PARQUET=ON \
        -DARROW_ORC=OFF \
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
        -DCMAKE_INSTALL_PREFIX=/tmp/arrow_install.7 \
        -DCMAKE_INSTALL_LIBDIR=lib \
        cpp

make -j$NPROC

make install

cd java
mvn clean install -P arrow-jni -pl dataset,gandiva -am -Darrow.cpp.build.dir=/tmp/arrow_install.7/lib -DskipTests -Dcheckstyle.skip
echo "Finish to build Arrow from Source !!!"
