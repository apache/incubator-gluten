#!/bin/bash

set -eux

CURRENT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
GLUTEN_DIR="$CURRENT_DIR/.."
LINUX_OS=$(. /etc/os-release && echo ${ID})
VERSION=$(. /etc/os-release && echo ${VERSION_ID})
ARCH=`uname -m`

if [[ -n "$JAVA_HOME" ]]; then
  echo "JAVA_HOME needs to be set for Gluten Compilation"
fi

cd "$GLUTEN_DIR"

# build gluten with velox backend, prompt always respond y
export PROMPT_ALWAYS_RESPOND=y
./dev/buildbundle-veloxbe.sh --enable_vcpkg=OFF --build_tests=ON --build_benchmarks=ON --enable_s3=ON  --enable_hdfs=ON "$@"

# make thirdparty package
./dev/build-thirdparty.sh
