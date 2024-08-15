#!/bin/bash

set -e

source /opt/rh/devtoolset-9/enable
cd $GITHUB_WORKSPACE/
source ./dev/vcpkg/env.sh
export NUM_THREADS=4
./dev/builddeps-veloxbe.sh --build_tests=OFF --build_benchmarks=OFF --build_arrow=OFF --enable_s3=ON \
                           --enable_gcs=ON --enable_hdfs=ON --enable_abfs=ON
