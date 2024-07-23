#!/bin/bash

set -ex

CURRENT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
GLUTEN_DIR="$CURRENT_DIR/.."

cd "$GLUTEN_DIR"
if [ "$LINUX_OS" == "centos" ]; then
  if [ "$VERSION" == "8" ]; then
    source /opt/rh/gcc-toolset-9/enable
  elif [ "$VERSION" == "7" ]; then
    source /opt/rh/devtoolset-9/enable
  fi
fi
source ./dev/vcpkg/env.sh
./dev/buildbundle-veloxbe.sh --build_tests=ON --build_benchmarks=ON --enable_s3=ON  --enable_hdfs=ON
