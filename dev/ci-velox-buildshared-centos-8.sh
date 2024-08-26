#!/bin/bash

set -e

source /opt/rh/gcc-toolset-9/enable
./dev/builddeps-veloxbe.sh --run_setup_script=OFF --enable_ep_cache=OFF --build_tests=ON \
    --build_examples=ON --build_benchmarks=ON --build_protobuf=ON
