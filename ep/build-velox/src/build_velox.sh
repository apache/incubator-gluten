#!/bin/bash

set -exu

GLUTEN_DIR=/opt/gluten
#Set on run gluten on S3
ENABLE_S3=OFF
#Set on run gluten on HDFS
ENABLE_HDFS=OFF
#It can be set to OFF when compiling velox again
BUILD_PROTOBUF=OFF
#It can be set to OFF when compiling velox again
BUILD_FOLLY=OFF
BUILD_TYPE=release

#for ep cache
VELOX_REPO=https://github.com/oap-project/velox.git
VELOX_BRANCH=main
TARGET_BUILD_COMMIT=""
ENABLE_EP_CACHE=OFF

for arg in "$@"
do
    case $arg in
        --gluten_dir=*)
        GLUTEN_DIR=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
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
        --build_protobuf=*)
        BUILD_PROTOBUF=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --build_folly=*)
        BUILD_FOLLY=("${arg#*=}")
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

VELOX_HOME="$GLUTEN_DIR/ep/build-velox/build/velox_ep/"
echo "Start building Velox..."
echo "CMAKE Arguments:"
echo "GLUTEN_DIR=${GLUTEN_DIR}"
echo "VELOX_HOME=${VELOX_HOME}"
echo "ENABLE_S3=${ENABLE_S3}"
echo "ENABLE_HDFS=${ENABLE_HDFS}"
echo "BUILD_PROTOBUF=${BUILD_PROTOBUF}"
echo "BUILD_FOLLY=${BUILD_FOLLY}"
echo "BUILD_TYPE=${BUILD_TYPE}"

if [ ! -d $VELOX_HOME ]; then
  echo "$VELOX_HOME is not exist!!!"
  exit 1
fi

function process_script {
    # make this function Reentrantly
    git checkout scripts/setup-ubuntu.sh
    sed -i '/protobuf-compiler/d' scripts/setup-ubuntu.sh
    sed -i '/^sudo --preserve-env apt install/a\  *thrift* \\' scripts/setup-ubuntu.sh
    sed -i '/^sudo --preserve-env apt install/a\  libiberty-dev \\' scripts/setup-ubuntu.sh
    sed -i '/^sudo --preserve-env apt install/a\  libxml2-dev \\' scripts/setup-ubuntu.sh
    sed -i '/^sudo --preserve-env apt install/a\  libkrb5-dev \\' scripts/setup-ubuntu.sh
    sed -i '/^sudo --preserve-env apt install/a\  libgsasl7-dev \\' scripts/setup-ubuntu.sh
    sed -i '/^sudo --preserve-env apt install/a\  libuuid1 \\' scripts/setup-ubuntu.sh
    sed -i '/^sudo --preserve-env apt install/a\  uuid-dev \\' scripts/setup-ubuntu.sh
    sed -i 's/^  liblzo2-dev.*/  liblzo2-dev \\/g' scripts/setup-ubuntu.sh
    sed -i 's/^  ninja -C "${BINARY_DIR}" install/  sudo ninja -C "${BINARY_DIR}" install/g' scripts/setup-helper-functions.sh
    if [ $BUILD_FOLLY == "ON" ]; then
      sed -i '/^function install_fmt.*/i function install_folly {\n  github_checkout facebook/folly v2022.07.11.00\n  cmake_install -DBUILD_TESTS=OFF\n}\n' scripts/setup-ubuntu.sh
      sed -i '/^  run_and_time install_fmt/a \ \ run_and_time install_folly' scripts/setup-ubuntu.sh
    fi
    if [ $BUILD_PROTOBUF == "ON" ]; then
      sed -i '/^function install_fmt.*/i function install_protobuf {\n  wget https://github.com/protocolbuffers/protobuf/releases/download/v21.4/protobuf-all-21.4.tar.gz\n  tar -xzf protobuf-all-21.4.tar.gz\n  cd protobuf-21.4\n  ./configure  CXXFLAGS="-fPIC"  --prefix=/usr/local\n  make "-j$(nproc)"\n  sudo make install\n  sudo ldconfig\n}\n' scripts/setup-ubuntu.sh
      sed -i '/^  run_and_time install_fmt/a \ \ run_and_time install_protobuf' scripts/setup-ubuntu.sh
    fi
    if [ $ENABLE_HDFS == "ON" ]; then
      sed -i '/^function install_fmt.*/i function install_libhdfs3 {\n  github_checkout apache/hawq master\n  cd depends/libhdfs3\n sed -i "/FIND_PACKAGE(GoogleTest REQUIRED)/d" ./CMakeLists.txt\n  sed -i "s/dumpversion/dumpfullversion/" ./CMake/Platform.cmake\n sed -i "s/dfs.domain.socket.path\\", \\"\\"/dfs.domain.socket.path\\", \\"\\/var\\/lib\\/hadoop-hdfs\\/dn_socket\\"/g" src/common/SessionConfig.cpp\n cmake_install\n}\n' scripts/setup-ubuntu.sh
      sed -i '/^  run_and_time install_fmt/a \ \ run_and_time install_libhdfs3' scripts/setup-ubuntu.sh
    fi
    if [ $ENABLE_S3 == "ON" ]; then
      sed -i '/^function install_fmt.*/i function install_awssdk {\n  github_checkout aws/aws-sdk-cpp 1.9.379 --depth 1 --recurse-submodules\n  cmake_install -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS:BOOL=OFF -DMINIMIZE_SIZE:BOOL=ON -DENABLE_TESTING:BOOL=OFF -DBUILD_ONLY:STRING="s3;identity-management" \n} \n' scripts/setup-ubuntu.sh
      sed -i '/^  run_and_time install_fmt/a \ \ run_and_time install_awssdk' scripts/setup-ubuntu.sh
    fi
    sed -i 's/-mavx2 -mfma -mavx -mf16c -mlzcnt -std=c++17/-march=native -std=c++17 -mno-avx512f/g' scripts/setup-helper-functions.sh
}

function compile {
    scripts/setup-ubuntu.sh
    COMPILE_OPTION="-DVELOX_ENABLE_PARQUET=ON -DVELOX_BUILD_TESTING=OFF -DVELOX_ENABLE_DUCKDB=OFF -DVELOX_BUILD_TEST_UTILS=ON"
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
    echo $TARGET_BUILD_COMMIT > "${BUILD_DIR}/velox-commit.cache"
}

function check_commit {
    COMMIT_ID="$(git ls-remote $VELOX_REPO $VELOX_BRANCH | awk '{print $1;}')"
    if $(git merge-base --is-ancestor $COMMIT_ID HEAD);
    then
      echo "Current branch contain lastest commit: $COMMIT_ID "
    else
      echo "Current branch do not contain lastest commit: $COMMIT_ID !"
      exit 1
    fi
}

function check_rp_cache {
  TARGET_BUILD_COMMIT="$(git ls-remote $VELOX_REPO $VELOX_BRANCH | awk '{print $1;}')"
  echo "Target Velox commit: $TARGET_BUILD_COMMIT"
  if [ $ENABLE_EP_CACHE == "ON" ]; then
    if [ -e ${BUILD_DIR}/arrow-commit.cache ]; then
      LAST_BUILT_COMMIT="$(cat ${BUILD_DIR}/velox-commit.cache)"
      if [ -n $LAST_BUILT_COMMIT ]; then
        if [ -z "$TARGET_BUILD_COMMIT" ]
          then
            echo "Unable to parse Velox commit: $TARGET_BUILD_COMMIT."
            exit 1
            fi
            if [ "$TARGET_BUILD_COMMIT" = "$LAST_BUILT_COMMIT" ]; then
                echo "Velox build of commit $TARGET_BUILD_COMMIT was cached, skipping build..."
                exit 0
            else
                echo "Found cached commit $LAST_BUILT_COMMIT for Velox which is different with target commit $TARGET_BUILD_COMMIT."
            fi
        fi
    fi
  fi

  if [ -e ${BUILD_DIR}/velox-commit.cache ]; then
      rm -f ${BUILD_DIR}/velox-commit.cache
  fi
}

function incremental_build {
  if [ $ENABLE_EP_CACHE == "ON" ] && [ -d $ARROW_SOURCE_DIR ]; then
    echo "Applying incremental build for Velox..."
    git init .
    EXISTS=`git show-ref refs/heads/build_$TARGET_BUILD_COMMIT || true`
    if [ -z "$EXISTS" ]; then
      git fetch $VELOX_REPO $TARGET_BUILD_COMMIT:build_$TARGET_BUILD_COMMIT
    fi
    git reset --hard HEAD
    git checkout build_$TARGET_BUILD_COMMIT
  fi
}

cd $VELOX_HOME
check_commit
check_rp_cache
process_script
compile

echo "Velox Installation finished."

