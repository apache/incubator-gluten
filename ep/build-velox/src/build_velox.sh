#!/bin/bash

set -exu
#Set on run gluten on S3
ENABLE_S3=OFF
#Set on run gluten on HDFS
ENABLE_HDFS=OFF
BUILD_TYPE=release
VELOX_HOME=""
ENABLE_EP_CACHE=OFF
ENABLE_BENCHMARK=OFF

LINUX_DISTRIBUTION=$(. /etc/os-release && echo ${ID})
LINUX_VERSION_ID=$(. /etc/os-release && echo ${VERSION_ID})

for arg in "$@"; do
  case $arg in
  --velox_home=*)
    VELOX_HOME=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --enable_s3=*)
    ENABLE_S3=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --enable_hdfs=*)
    ENABLE_HDFS=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --build_type=*)
    BUILD_TYPE=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --enable_ep_cache=*)
    ENABLE_EP_CACHE=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --build_benchmarks=*)
    ENABLE_BENCHMARK=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  *)
    OTHER_ARGUMENTS+=("$1")
    shift # Remove generic argument from processing
    ;;
  esac
done

function compile {
  TARGET_BUILD_COMMIT=$(git rev-parse --verify HEAD)
  if [[ "$LINUX_DISTRIBUTION" == "ubuntu" || "$LINUX_DISTRIBUTION" == "debian" ]]; then
    scripts/setup-ubuntu.sh
  elif [[ "$LINUX_DISTRIBUTION" == "centos" ]]; then
    case "$LINUX_VERSION_ID" in
      8) scripts/setup-centos8.sh ;;
      7)
        scripts/setup-centos7.sh
        set +u
        export PKG_CONFIG_PATH=/usr/local/lib64/pkgconfig:/usr/local/lib/pkgconfig:/usr/lib64/pkgconfig:/usr/lib/pkgconfig:$PKG_CONFIG_PATH
        source /opt/rh/devtoolset-9/enable
        set -u
      ;;
      *)
        echo "Unsupport centos version: $LINUX_VERSION_ID"
        exit 1
      ;;
    esac
  elif [[ "$LINUX_DISTRIBUTION" == "alinux" ]]; then
    case "$LINUX_VERSION_ID" in
      3) scripts/setup-centos8.sh ;;
      *)
        echo "Unsupport alinux version: $LINUX_VERSION_ID"
        exit 1
      ;;
    esac
  else
    echo "Unsupport linux distribution: $LINUX_DISTRIBUTION"
    exit 1
  fi

  COMPILE_OPTION="-DVELOX_ENABLE_PARQUET=ON"
  if [ $ENABLE_BENCHMARK == "OFF" ]; then
    COMPILE_OPTION="$COMPILE_OPTION -DVELOX_BUILD_TESTING=OFF -DVELOX_ENABLE_DUCKDB=OFF -DVELOX_BUILD_TEST_UTILS=ON"
  fi
  if [ $ENABLE_HDFS == "ON" ]; then
    COMPILE_OPTION="$COMPILE_OPTION -DVELOX_ENABLE_HDFS=ON"
  fi
  if [ $ENABLE_S3 == "ON" ]; then
    COMPILE_OPTION="$COMPILE_OPTION -DVELOX_ENABLE_S3=ON"
  fi
  COMPILE_OPTION="$COMPILE_OPTION -DCMAKE_BUILD_TYPE=${BUILD_TYPE}"
  COMPILE_TYPE=$(if [[ "$BUILD_TYPE" == "debug" ]] || [[ "$BUILD_TYPE" == "Debug" ]]; then echo 'debug'; else echo 'release'; fi)
  echo "COMPILE_OPTION: "$COMPILE_OPTION
  make $COMPILE_TYPE EXTRA_CMAKE_FLAGS="${COMPILE_OPTION}"
  echo "INSTALL xsimd gtest."
  cd _build/$COMPILE_TYPE/_deps
  sudo cmake --install xsimd-build/
  sudo cmake --install gtest-build/
}

function check_commit {
  if [ $ENABLE_EP_CACHE == "ON" ]; then
    if [ -f ${BUILD_DIR}/velox-commit.cache ]; then
      CACHED_BUILT_COMMIT="$(cat ${BUILD_DIR}/velox-commit.cache)"
      if [ -n "$CACHED_BUILT_COMMIT" ]; then
        if [ "$TARGET_BUILD_COMMIT" = "$CACHED_BUILT_COMMIT" ]; then
          echo "Velox build of commit $TARGET_BUILD_COMMIT was cached."
          exit 0
        else
          echo "Found cached commit $CACHED_BUILT_COMMIT for Velox which is different with target commit $TARGET_BUILD_COMMIT."
        fi
      fi
    fi
  else
    git clean -dffx :/
  fi

  if [ -f ${BUILD_DIR}/velox-commit.cache ]; then
    rm -f ${BUILD_DIR}/velox-commit.cache
  fi
}

CURRENT_DIR=$(
  cd "$(dirname "$BASH_SOURCE")"
  pwd
)
if [ "$VELOX_HOME" == "" ]; then
  VELOX_HOME="$CURRENT_DIR/../build/velox_ep"
fi

BUILD_DIR="$CURRENT_DIR/../build"
echo "Start building Velox..."
echo "CMAKE Arguments:"
echo "VELOX_HOME=${VELOX_HOME}"
echo "ENABLE_S3=${ENABLE_S3}"
echo "ENABLE_HDFS=${ENABLE_HDFS}"
echo "BUILD_TYPE=${BUILD_TYPE}"

if [ ! -d $VELOX_HOME ]; then
  echo "$VELOX_HOME is not exist!!!"
  exit 1
fi

cd $VELOX_HOME
TARGET_BUILD_COMMIT="$(git rev-parse --verify HEAD)"
if [ -z "$TARGET_BUILD_COMMIT" ]; then
  echo "Unable to parse Velox commit: $TARGET_BUILD_COMMIT."
  exit 0
fi
echo "Target Velox commit: $TARGET_BUILD_COMMIT"

check_commit
compile

echo "Successfully built Velox from Source."
echo $TARGET_BUILD_COMMIT >"${BUILD_DIR}/velox-commit.cache"
