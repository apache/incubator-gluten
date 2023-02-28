#!/bin/bash

set -exu
#Set on run gluten on S3
ENABLE_S3=OFF
#Set on run gluten on HDFS
ENABLE_HDFS=OFF
#It can be set to OFF when compiling velox again
BUILD_PROTOBUF=ON
BUILD_TYPE=release
VELOX_HOME=
#for ep cache
VELOX_REPO=https://github.com/oap-project/velox.git
VELOX_BRANCH=main
TARGET_BUILD_COMMIT=""

LINUX_DISTRIBUTION=$(. /etc/os-release && echo ${ID})

for arg in "$@"
do
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
        --build_protobuf=*)
        BUILD_PROTOBUF=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --build_type=*)
        BUILD_TYPE=("${arg#*=}")
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
      if [ $ENABLE_HDFS == "ON" ]; then
        sed -i '/^function install_fmt.*/i function install_libhdfs3 {\n  github_checkout apache/hawq master\n  cd depends/libhdfs3\n sed -i "/FIND_PACKAGE(GoogleTest REQUIRED)/d" ./CMakeLists.txt\n  sed -i "s/dumpversion/dumpfullversion/" ./CMake/Platform.cmake\n sed -i "s/dfs.domain.socket.path\\", \\"\\"/dfs.domain.socket.path\\", \\"\\/var\\/lib\\/hadoop-hdfs\\/dn_socket\\"/g" src/common/SessionConfig.cpp\n sed -i "s/pos < endOfCurBlock/pos \\< endOfCurBlock \\&\\& pos \\- cursor \\<\\= 128 \\* 1024/g" src/client/InputStreamImpl.cpp\n cmake_install\n}\n' scripts/setup-ubuntu.sh
        sed -i '/^  run_and_time install_fmt/a \ \ run_and_time install_libhdfs3' scripts/setup-ubuntu.sh
      fi
      if [ $BUILD_PROTOBUF == "ON" ]; then
        sed -i '/^function install_fmt.*/i function install_protobuf {\n  wget https://github.com/protocolbuffers/protobuf/releases/download/v21.4/protobuf-all-21.4.tar.gz\n  tar -xzf protobuf-all-21.4.tar.gz\n  cd protobuf-21.4\n  ./configure  CXXFLAGS="-fPIC"  --prefix=/usr/local\n  make "-j$(nproc)"\n  sudo make install\n  sudo ldconfig\n}\n' scripts/setup-ubuntu.sh
        sed -i '/^  run_and_time install_fmt/a \ \ run_and_time install_protobuf' scripts/setup-ubuntu.sh
      fi
      if [ $ENABLE_S3 == "ON" ]; then
        sed -i '/^function install_fmt.*/i function install_awssdk {\n  github_checkout aws/aws-sdk-cpp 1.9.379 --depth 1 --recurse-submodules\n  cmake_install -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS:BOOL=OFF -DMINIMIZE_SIZE:BOOL=ON -DENABLE_TESTING:BOOL=OFF -DBUILD_ONLY:STRING="s3;identity-management" \n} \n' scripts/setup-ubuntu.sh
        sed -i '/^  run_and_time install_fmt/a \ \ run_and_time install_awssdk' scripts/setup-ubuntu.sh
      fi
      sed -i 's/run_and_time install_conda/#run_and_time install_conda/' scripts/setup-ubuntu.sh
      
}

function process_setup_centos8 {
      # make this function Reentrantly
      git checkout scripts/setup-centos8.sh
      sed -i '/^function dnf_install/i\DEPENDENCY_DIR=${DEPENDENCY_DIR:-$(pwd)}' scripts/setup-centos8.sh
      sed -i '/^dnf_install autoconf/a\dnf_install libxml2-devel libgsasl-devel libuuid-devel' scripts/setup-centos8.sh

      # install gtest
      sed -i '/^cmake_install_deps gflags/i function install_gtest {\n  wget https://github.com/google/googletest/archive/refs/tags/release-1.12.1.tar.gz\n  tar -xzf release-1.12.1.tar.gz\n  cd googletest-release-1.12.1\n  mkdir build && cd build && cmake -DBUILD_GTEST=ON -DBUILD_GMOCK=ON -DINSTALL_GTEST=ON -DINSTALL_GMOCK=ON -DBUILD_SHARED_LIBS=ON ..\n  make "-j$(nproc)"\n  make install\n  cd ../../ && ldconfig\n}\n' scripts/setup-centos8.sh
      sed -i '/^cmake_install_deps gflags/i FB_OS_VERSION=v2022.11.14.00\nfunction install_folly {\n  github_checkout facebook/folly "${FB_OS_VERSION}"\n  cmake_install -DBUILD_TESTS=OFF\n}\n'  scripts/setup-centos8.sh
      sed -i '/^cmake_install_deps fmt/a \ \ install_gtest' scripts/setup-centos8.sh
      sed -i '/^cmake_install_deps fmt/a \install_folly' scripts/setup-centos8.sh

      if [ $ENABLE_HDFS == "ON" ]; then
        sed -i '/^cmake_install_deps gflags/i function install_libhdfs3 {\n  github_checkout apache/hawq master\n  cd depends/libhdfs3\n sed -i "/FIND_PACKAGE(GoogleTest REQUIRED)/d" ./CMakeLists.txt\n  sed -i "s/dumpversion/dumpfullversion/" ./CMake/Platform.cmake\n sed -i "s/dfs.domain.socket.path\\", \\"\\"/dfs.domain.socket.path\\", \\"\\/var\\/lib\\/hadoop-hdfs\\/dn_socket\\"/g" src/common/SessionConfig.cpp\n sed -i "s/pos < endOfCurBlock/pos \\< endOfCurBlock \\&\\& pos \\- cursor \\<\\= 128 \\* 1024/g" src/client/InputStreamImpl.cpp\n cmake_install\n}\n' scripts/setup-centos8.sh
        sed -i '/^cmake_install_deps fmt/a \ \ install_libhdfs3' scripts/setup-centos8.sh
      fi
      if [[ $BUILD_PROTOBUF == "ON" ]] || [[ $ENABLE_HDFS == "ON" ]]; then
        sed -i '/^cmake_install_deps gflags/i function install_protobuf {\n  wget https://github.com/protocolbuffers/protobuf/releases/download/v21.4/protobuf-all-21.4.tar.gz\n  tar -xzf protobuf-all-21.4.tar.gz\n  cd protobuf-21.4\n  ./configure  CXXFLAGS="-fPIC"  --prefix=/usr/local\n  make "-j$(nproc)"\n  make install\n  cd ../../ && ldconfig\n}\n' scripts/setup-centos8.sh
        sed -i '/^cmake_install_deps fmt/a \ \ install_protobuf' scripts/setup-centos8.sh
      fi
      if [ $ENABLE_S3 == "ON" ]; then
        sed -i '/^cmake_install_deps gflags/i function install_awssdk {\n  github_checkout aws/aws-sdk-cpp 1.9.379 --depth 1 --recurse-submodules\n  cmake_install -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS:BOOL=OFF -DMINIMIZE_SIZE:BOOL=ON -DENABLE_TESTING:BOOL=OFF -DBUILD_ONLY:STRING="s3;identity-management" \n} \n' scripts/setup-centos8.sh
        sed -i '/^cmake_install_deps fmt/a \ \ install_awssdk' scripts/setup-centos8.sh
      fi
}

CURRENT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
if [ "$VELOX_HOME" == "" ]; then
  VELOX_HOME="$CURRENT_DIR/../build/velox_ep"
fi

BUILD_DIR="$CURRENT_DIR/../build"
echo "Start building Velox..."
echo "CMAKE Arguments:"
echo "VELOX_HOME=${VELOX_HOME}"
echo "ENABLE_S3=${ENABLE_S3}"
echo "ENABLE_HDFS=${ENABLE_HDFS}"
echo "BUILD_PROTOBUF=${BUILD_PROTOBUF}"
echo "BUILD_TYPE=${BUILD_TYPE}"

if [ ! -d $VELOX_HOME ]; then
  echo "$VELOX_HOME is not exist!!!"
  exit 1
fi

function process_script {
    sed -i 's/^  ninja -C "${BINARY_DIR}" install/  sudo ninja -C "${BINARY_DIR}" install/g' scripts/setup-helper-functions.sh
    sed -i 's/-mavx2 -mfma -mavx -mf16c -mlzcnt -std=c++17/-march=native -std=c++17 -mno-avx512f/g' scripts/setup-helper-functions.sh
    if [[ "$LINUX_DISTRIBUTION" == "ubuntu" || "$LINUX_DISTRIBUTION" == "debian" ]]; then
      process_setup_ubuntu
    else # Assume CentOS
      process_setup_centos8
    fi
}

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

cd $VELOX_HOME
check_commit
process_script
compile

echo "Velox Installation finished."

