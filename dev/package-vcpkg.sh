#!/bin/bash

set -ex

CURRENT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
GLUTEN_DIR="$CURRENT_DIR/.."

cd "$GLUTEN_DIR"
source ./dev/vcpkg/env.sh
./dev/buildbundle-veloxbe.sh --build_tests=ON --build_benchmarks=ON --enable_s3=ON  --enable_hdfs=ON
