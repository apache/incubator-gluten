#!/bin/bash

set -exu
#Set on run gluten on S3
ENABLE_S3=OFF
#Set on run gluten on HDFS
ENABLE_HDFS=OFF
BUILD_TYPE=release
VELOX_HOME=""
ENABLE_EP_CACHE=OFF
ARROW_HOME=""
THRIFT_HOME=""

LINUX_DISTRIBUTION=$(. /etc/os-release && echo ${ID})

for arg in "$@"; do
  case $arg in
  --velox_home=*)
    VELOX_HOME=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --arrow_home=*)
    ARROW_HOME=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --thrift_home=*)
    THRIFT_HOME=("${arg#*=}")
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
  else # Assume CentOS
    scripts/setup-centos8.sh
  fi
  COMPILE_OPTION="-DVELOX_ENABLE_PARQUET=ON -DVELOX_BUILD_TESTING=OFF -DVELOX_ENABLE_DUCKDB=OFF -DVELOX_BUILD_TEST_UTILS=ON"
  if [ $ENABLE_HDFS == "ON" ]; then
    COMPILE_OPTION="$COMPILE_OPTION -DVELOX_ENABLE_HDFS=ON"
  fi
  if [ $ENABLE_S3 == "ON" ]; then
    COMPILE_OPTION="$COMPILE_OPTION -DVELOX_ENABLE_S3=ON"
  fi
  if [ -n "$ARROW_HOME" ]; then
    if [ ! -d "$ARROW_HOME" ]; then
      echo "ARROW_HOME=$ARROW_HOME not found" >&2
      exit 1
    fi
    COMPILE_OPTION="$COMPILE_OPTION -DVELOX_ARROW_HOME=$ARROW_HOME"
  fi
  if [ -n "$THRIFT_HOME" ]; then
    if [ ! -d "$THRIFT_HOME" ]; then
      echo "THRIFT_HOME=$THRIFT_HOME not found" >&2
      exit 1
    fi
    COMPILE_OPTION="$COMPILE_OPTION -DVELOX_THRIFT_HOME=$THRIFT_HOME"
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
    git clean -dfx :/
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

if [ -z "$ARROW_HOME" ]; then
  ARROW_HOME=$CURRENT_DIR/../../build-arrow/build/arrow_install
  [ -d "$ARROW_HOME" ] || ARROW_HOME=""
fi

if [ -z "$THRIFT_HOME" ]; then
  THRIFT_HOME=$CURRENT_DIR/../../build-arrow/build/arrow_ep/cpp/build/thrift_ep-install
  [ -d "$THRIFT_HOME" ] || THRIFT_HOME=""
fi

BUILD_DIR="$CURRENT_DIR/../build"
echo "Start building Velox..."
echo "CMAKE Arguments:"
echo "VELOX_HOME=${VELOX_HOME}"
echo "ARROW_HOME=${ARROW_HOME}"
echo "THRIFT_HOME=${THRIFT_HOME}"
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
