#!/bin/bash

set -eux

CURRENT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
GLUTEN_DIR="$CURRENT_DIR/.."
LINUX_OS=$(. /etc/os-release && echo ${ID})
VERSION=$(. /etc/os-release && echo ${VERSION_ID})
ARCH=`uname -m`

cd "$GLUTEN_DIR"
if [ "$LINUX_OS" == "centos" ]; then
  if [ "$VERSION" == "8" ]; then
    source /opt/rh/gcc-toolset-9/enable
  elif [ "$VERSION" == "7" ]; then
    source /opt/rh/devtoolset-9/enable
  fi
fi

# build gluten with velox backend, prompt always respond y
export PROMPT_ALWAYS_RESPOND=y
./dev/buildbundle-veloxbe.sh --build_tests=ON --build_benchmarks=ON --enable_s3=ON  --enable_hdfs=ON "$@"

# make thirdparty package
./dev/build-thirdparty.sh
