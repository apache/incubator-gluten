#!/bin/bash

set -e

source /opt/rh/devtoolset-11/enable
export NUM_THREADS=4
./dev/builddeps-veloxbe.sh --enable_vcpkg=ON --build_arrow=ON --build_tests=OFF --build_benchmarks=OFF \
                           --build_examples=OFF --enable_s3=ON --enable_gcs=ON --enable_hdfs=ON --enable_abfs=ON
