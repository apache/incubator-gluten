#!/bin/bash

set -e

source /opt/rh/devtoolset-9/enable
export NUM_THREADS=4
./dev/builddeps-veloxbe.sh --enable_vcpkg=ON --build_arrow=OFF --build_tests=ON --build_benchmarks=ON \
                           --build_examples=ON --enable_s3=ON --enable_gcs=ON --enable_hdfs=ON --enable_abfs=ON
