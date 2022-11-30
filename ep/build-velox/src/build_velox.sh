#!/bin/bash

set -eu

NPROC=$(nproc)

BUILD_VELOX_FROM_SOURCE=OFF
COMPILE_VELOX=OFF
ENABLE_EP_CACHE=OFF
ENABLE_S3=OFF
ENABLE_HDFS=OFF
BUILD_PROTOBUF=ON
BUILD_FOLLY=ON
VELOX_BUILD_TYPE=release
VELOX_HOME=/root/velox

VELOX_REPO=https://github.com/oap-project/velox.git
VELOX_BRANCH=main

LINUX_DISTRIBUTION=$(. /etc/os-release && echo ${ID})

for arg in "$@"
do
    case $arg in
        --build_velox_from_source=*)
        BUILD_VELOX_FROM_SOURCE=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --compile_velox=*)
        COMPILE_VELOX=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --enable_ep_cache=*)
        ENABLE_EP_CACHE=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --velox_enable_s3=*)
        ENABLE_S3=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --velox_enable_hdfs=*)
        ENABLE_HDFS=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --build_protobuf=*)
        BUILD_PROTOBUF=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --velox_build_folly=*)
        BUILD_FOLLY=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --velox_build_type=*)
        VELOX_BUILD_TYPE=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --velox_home=*)
        VELOX_HOME=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
	*)
	OTHER_ARGUMENTS+=("$1")
        shift # Remove generic argument from processing
        ;;
    esac
done
function process_setup_ubuntu {
      # make this function Reentrantly
      git checkout scripts/setup-ubuntu.sh
      sed -i '/libprotobuf-dev/d' scripts/setup-ubuntu.sh
      sed -i '/protobuf-compiler/d' scripts/setup-ubuntu.sh
      sed -i '/^sudo --preserve-env apt install/a\  *thrift* \\' scripts/setup-ubuntu.sh
      sed -i '/^sudo --preserve-env apt install/a\  libiberty-dev \\' scripts/setup-ubuntu.sh
      sed -i '/^sudo --preserve-env apt install/a\  libxml2-dev \\' scripts/setup-ubuntu.sh
      sed -i '/^sudo --preserve-env apt install/a\  libkrb5-dev \\' scripts/setup-ubuntu.sh
      sed -i '/^sudo --preserve-env apt install/a\  libgsasl7-dev \\' scripts/setup-ubuntu.sh
      sed -i '/^sudo --preserve-env apt install/a\  libuuid1 \\' scripts/setup-ubuntu.sh
      sed -i '/^sudo --preserve-env apt install/a\  uuid-dev \\' scripts/setup-ubuntu.sh
      sed -i '/^sudo --preserve-env apt install/a\  libiberty-dev \\' scripts/setup-ubuntu.sh
      sed -i 's/^  liblzo2-dev.*/  liblzo2-dev \\/g' scripts/setup-ubuntu.sh
      if [ $BUILD_FOLLY == "ON" ]; then
        sed -i '/^function install_fmt.*/i function install_folly {\n  github_checkout facebook/folly v2022.07.11.00\n  cmake_install -DBUILD_TESTS=OFF\n}\n' scripts/setup-ubuntu.sh
        sed -i '/^  run_and_time install_fmt/a \ \ run_and_time install_folly' scripts/setup-ubuntu.sh
      fi
      if [ $ENABLE_HDFS == "ON" ]; then
        sed -i '/^function install_fmt.*/i function install_libhdfs3 {\n  github_checkout apache/hawq master\n  cd depends/libhdfs3\n sed -i "/FIND_PACKAGE(GoogleTest REQUIRED)/d" ./CMakeLists.txt\n  sed -i "s/dumpversion/dumpfullversion/" ./CMake/Platform.cmake\n sed -i "s/dfs.domain.socket.path\\", \\"\\"/dfs.domain.socket.path\\", \\"\\/var\\/lib\\/hadoop-hdfs\\/dn_socket\\"/g" src/common/SessionConfig.cpp\n cmake_install\n}\n' scripts/setup-ubuntu.sh
        sed -i '/^  run_and_time install_fmt/a \ \ run_and_time install_libhdfs3' scripts/setup-ubuntu.sh
      fi
      if [[ $BUILD_PROTOBUF == "ON" ]] || [[ $ENABLE_HDFS == "ON" ]]; then
        sed -i '/^function install_fmt.*/i function install_protobuf {\n  wget https://github.com/protocolbuffers/protobuf/releases/download/v21.4/protobuf-all-21.4.tar.gz\n  tar -xzf protobuf-all-21.4.tar.gz\n  cd protobuf-21.4\n  ./configure  CXXFLAGS="-fPIC"  --prefix=/usr/local\n  make "-j$(nproc)"\n  sudo make install\n  sudo ldconfig\n}\n' scripts/setup-ubuntu.sh
        sed -i '/^  run_and_time install_fmt/a \ \ run_and_time install_protobuf' scripts/setup-ubuntu.sh
      fi
      if [ $ENABLE_S3 == "ON" ]; then
        sed -i '/^function install_fmt.*/i function install_awssdk {\n  github_checkout aws/aws-sdk-cpp 1.9.379 --depth 1 --recurse-submodules\n  cmake_install -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS:BOOL=OFF -DMINIMIZE_SIZE:BOOL=ON -DENABLE_TESTING:BOOL=OFF -DBUILD_ONLY:STRING="s3;identity-management" \n} \n' scripts/setup-ubuntu.sh
        sed -i '/^  run_and_time install_fmt/a \ \ run_and_time install_awssdk' scripts/setup-ubuntu.sh
      fi
}

function process_setup_centos8 {
      # make this function Reentrantly
      git checkout scripts/setup-centos8.sh
      sed -i '/^dnf_install autoconf/a\dnf_install libxml2-devel libgsasl-devel libuuid-devel' scripts/setup-centos8.sh

      if [ $BUILD_FOLLY == "ON" ]; then
        sed -i '/^cmake_install_deps gflags/i function install_folly {\n  github_checkout facebook/folly v2022.07.11.00\n  cmake_install -DBUILD_TESTS=OFF\n}\n' scripts/setup-centos8.sh
        sed -i '/^cmake_install_deps fmt/a \ \ run_and_time install_folly' scripts/setup-centos8.sh
      fi
      if [ $ENABLE_HDFS == "ON" ]; then
        sed -i '/^cmake_install_deps gflags/i function install_libhdfs3 {\n  github_checkout apache/hawq master\n  cd depends/libhdfs3\n sed -i "/FIND_PACKAGE(GoogleTest REQUIRED)/d" ./CMakeLists.txt\n  sed -i "s/dumpversion/dumpfullversion/" ./CMake/Platform.cmake\n sed -i "s/dfs.domain.socket.path\\", \\"\\"/dfs.domain.socket.path\\", \\"\\/var\\/lib\\/hadoop-hdfs\\/dn_socket\\"/g" src/common/SessionConfig.cpp\n cmake_install\n}\n' scripts/setup-centos8.sh
        sed -i '/^cmake_install_deps fmt/a \ \ run_and_time install_libhdfs3' scripts/setup-centos8.sh
      fi
      if [[ $BUILD_PROTOBUF == "ON" ]] || [[ $ENABLE_HDFS == "ON" ]]; then
        sed -i '/^cmake_install_deps gflags/i function install_protobuf {\n  wget https://github.com/protocolbuffers/protobuf/releases/download/v21.4/protobuf-all-21.4.tar.gz\n  tar -xzf protobuf-all-21.4.tar.gz\n  cd protobuf-21.4\n  ./configure  CXXFLAGS="-fPIC"  --prefix=/usr/local\n  make "-j$(nproc)"\n  sudo make install\n  sudo ldconfig\n}\n' scripts/setup-centos8.sh
        sed -i '/^cmake_install_deps fmt/a \ \ run_and_time install_protobuf' scripts/setup-centos8.sh
      fi
      if [ $ENABLE_S3 == "ON" ]; then
        sed -i '/^cmake_install_deps gflags/i function install_awssdk {\n  github_checkout aws/aws-sdk-cpp 1.9.379 --depth 1 --recurse-submodules\n  cmake_install -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS:BOOL=OFF -DMINIMIZE_SIZE:BOOL=ON -DENABLE_TESTING:BOOL=OFF -DBUILD_ONLY:STRING="s3;identity-management" \n} \n' scripts/setup-centos8.sh
        sed -i '/^cmake_install_deps fmt/a \ \ run_and_time install_awssdk' scripts/setup-centos8.sh
      fi
}

function process_script {
    sed -i 's/^  ninja -C "${BINARY_DIR}" install/  sudo ninja -C "${BINARY_DIR}" install/g' scripts/setup-helper-functions.sh
    sed -i 's/-mavx2 -mfma -mavx -mf16c -mlzcnt -std=c++17/-march=native -std=c++17 -mno-avx512f/g' scripts/setup-helper-functions.sh
    process_setup_ubuntu
    if [[ "$LINUX_DISTRIBUTION" == "ubuntu" ]]; then
      process_setup_ubuntu
    else # Assume CentOS
      process_setup_centos8
    fi
}

function compile {
    if [[ "$LINUX_DISTRIBUTION" == "ubuntu" ]]; then
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
    COMPILE_OPTION="$COMPILE_OPTION -DCMAKE_BUILD_TYPE=${VELOX_BUILD_TYPE}"
    COMPILE_TYPE=$(if [[ "$VELOX_BUILD_TYPE" == "debug" ]] || [[ "$VELOX_BUILD_TYPE" == "Debug" ]]; then echo 'debug'; else echo 'release'; fi)
    echo "COMPILE_OPTION: "$COMPILE_OPTION
    make $COMPILE_TYPE EXTRA_CMAKE_FLAGS="${COMPILE_OPTION}"
}

echo "Velox Installation"

CURRENT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
BUILD_DIR="$CURRENT_DIR/../build"
echo $CURRENT_DIR
echo $BUILD_DIR

cd ${CURRENT_DIR}


if [ $BUILD_VELOX_FROM_SOURCE == "ON" ]; then
    mkdir -p $BUILD_DIR
    TARGET_BUILD_COMMIT="$(git ls-remote $VELOX_REPO $VELOX_BRANCH | awk '{print $1;}')"
    echo "Target Velox commit: $TARGET_BUILD_COMMIT"
    if [ $ENABLE_EP_CACHE == "ON" ]; then
        if [ -e ${BUILD_DIR}/velox-commit.cache ]; then
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

    VELOX_PREFIX="${BUILD_DIR}" # Use build directory as VELOX_PREFIX
    VELOX_SOURCE_DIR="${VELOX_PREFIX}/velox_ep"
    VELOX_INSTALL_DIR="${VELOX_PREFIX}/velox_install"

    echo "VELOX_PREFIX=${VELOX_PREFIX}"
    echo "VELOX_SOURCE_DIR=${VELOX_SOURCE_DIR}"
    echo "VELOX_INSTALL_DIR=${VELOX_INSTALL_DIR}"

    if [ -d $VELOX_INSTALL_DIR ]; then
        rm -rf $VELOX_INSTALL_DIR
    fi

    if [ $ENABLE_EP_CACHE == "ON" ] && [ -d $VELOX_SOURCE_DIR ]; then
        echo "Applying incremental build for Velox..."
        pushd $VELOX_SOURCE_DIR
        git init .
        EXISTS=`git show-ref refs/heads/build_$TARGET_BUILD_COMMIT || true`
        if [ -z "$EXISTS" ]; then
            git fetch $VELOX_REPO $TARGET_BUILD_COMMIT:build_$TARGET_BUILD_COMMIT
        fi
        git reset --hard HEAD
        git checkout build_$TARGET_BUILD_COMMIT
    else
        echo "Creating brand-new build for Velox..."
        if [ -d $VELOX_SOURCE_DIR ]; then
            rm -rf $VELOX_SOURCE_DIR
        fi
        git clone $VELOX_REPO -b $VELOX_BRANCH $VELOX_SOURCE_DIR
        pushd $VELOX_SOURCE_DIR
        git checkout $TARGET_BUILD_COMMIT
    fi
    #sync submodules
    git submodule sync --recursive
    git submodule update --init --recursive

    export PROMPT_ALWAYS_RESPOND=n
    process_script
    compile
    echo "Successfully built Velox from Source !!!"
    echo $TARGET_BUILD_COMMIT > "${BUILD_DIR}/velox-commit.cache"
else
    VELOX_SOURCE_DIR=${VELOX_HOME}
    if [ $COMPILE_VELOX == "ON" ]; then
        echo "VELOX_SOURCE_DIR=${VELOX_SOURCE_DIR}"
        pushd $VELOX_SOURCE_DIR
        export PROMPT_ALWAYS_RESPOND=n
        process_script
        compile
    fi
    echo "Use existing Velox at ${VELOX_SOURCE_DIR}."
fi

