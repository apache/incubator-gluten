#!/bin/bash

set -exu

CURRENT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
BUILD_DIR="$CURRENT_DIR/../build"

# Set ON to run gluten with S3 support
ENABLE_S3=${ENABLE_S3:-'OFF'}
# Set ON to run gluten with HDFS support
ENABLE_HDFS=${ENABLE_HDFS:-'OFF'}
# Can be set OFF when recompiling Velox
BUILD_PROTOBUF=${BUILD_PROTOBUF:-'ON'}
BUILD_TYPE=${BUILD_TYPE:-'release'}
# Optimize build instruction set for arch
# Leave native if build time and run time arch are the same.
# ref. https://gcc.gnu.org/onlinedocs/gcc/x86-Options.html
BUILD_MARCH=${BUILD_MARCH:-'native'}
# Number of procesors available
BUILD_NPROC=${BUILD_NPROC:-$(getconf _NPROCESSORS_ONLN)}
BUILD_MAX_HIGH_MEM_JOBS=${BUILD_MAX_HIGH_MEM_JOBS:-$BUILD_NPROC}
BUILD_MAX_LINK_JOBS=${BUILD_MAX_LINK_JOBS:-$BUILD_NPROC}
# Read velox home from env or use default
VELOX_HOME=${VELOX_HOME:-"$CURRENT_DIR/../build/velox_ep"}
# Used for ep cache
VELOX_REPO=${VELOX_REPO:-'https://github.com/oap-project/velox.git'}
VELOX_BRANCH=${VELOX_BRANCH:-'main'}
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
        --build_march=*)
        BUILD_MARCH=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --build_nproc=*)
        BUILD_NPROC=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --build_max_high_mem_jobs=*)
        BUILD_MAX_HIGH_MEM_JOBS=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --build_max_link_jobs=*)
        BUILD_MAX_LINK_JOBS=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        *)
        OTHER_ARGUMENTS+=("$1")
        shift # Remove generic argument from processing
        ;;
    esac
done

function install_awssdk {
  github_checkout aws/aws-sdk-cpp 1.9.379 --depth 1 --recurse-submodules
  cmake_install -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS:BOOL=OFF -DMINIMIZE_SIZE:BOOL=ON -DENABLE_TESTING:BOOL=OFF -DBUILD_ONLY:STRING="s3;identity-management"
}

function install_libhdfs3 {
  github_checkout apache/hawq master
  cd depends/libhdfs3
  sed -i "/FIND_PACKAGE(GoogleTest REQUIRED)/d" ./CMakeLists.txt
  sed -i "s/dumpversion/dumpfullversion/" ./CMake/Platform.cmake
  sed -i "s/dfs.domain.socket.path\\", \\"\\"/dfs.domain.socket.path\\", \\"\\/var\\/lib\\/hadoop-hdfs\\/dn_socket\\"/g" src/common/SessionConfig.cpp
  sed -i "s/pos < endOfCurBlock/pos \\< endOfCurBlock \\&\\& pos \\- cursor \\<\\= 128 \\* 1024/g" src/client/InputStreamImpl.cpp
  cmake_install
}

function install_protobuf {
    wget https://github.com/protocolbuffers/protobuf/releases/download/v21.4/protobuf-all-21.4.tar.gz
    tar -xzf protobuf-all-21.4.tar.gz
    cd protobuf-21.4
    ./configure  CXXFLAGS="-fPIC"  --prefix=/usr/local
    make "-j${BUILD_NPROC}"
    sudo make install
    sudo ldconfig
}

  # install dependencies and source velox
function install_velox_deps_ubuntu {
  git checkout scripts/setup-ubuntu.sh
  source scripts/setup-ubuntu.sh
  sudo --preserve-env apt update && sudo apt install -y \
    *thrift* \
    libiberty-dev \
    libxml2-dev \
    libkrb5-dev \
    libgsasl7-dev \
    libuuid1 \
    uuid-dev \
    libiberty-dev
  
  run_and_time install_fmt
  [ $ENABLE_HDFS == "ON" ]    && run_and_time install_libhdfs3
  [ $BUILD_PROTOBUF == "ON" ] && run_and_time install_protobuf
  [ $ENABLE_S3 == "ON" ]      && run_and_time install_awssdk
  run_and_time install_folly
}

function process_setup_centos8 {
      # make this function Reentrantly
      git checkout scripts/setup-centos8.sh
      sed -i '/^function dnf_install/i\DEPENDENCY_DIR=${DEPENDENCY_DIR:-$(pwd)}' scripts/setup-centos8.sh
      sed -i '/^dnf_install autoconf/a\dnf_install libxml2-devel libgsasl-devel libuuid-devel' scripts/setup-centos8.sh

      # install gtest
      sed -i '/^cmake_install_deps gflags/i function install_gtest {\n  wget https://github.com/google/googletest/archive/refs/tags/release-1.12.1.tar.gz\n  tar -xzf release-1.12.1.tar.gz\n  cd googletest-release-1.12.1\n  mkdir build && cd build && cmake -DBUILD_GTEST=ON -DBUILD_GMOCK=ON -DINSTALL_GTEST=ON -DINSTALL_GMOCK=ON -DBUILD_SHARED_LIBS=ON ..\n  make "-j$(nproc)"\n  make install\n  cd ../../ && ldconfig\n}\n' scripts/setup-centos8.sh
      sed -i '/^cmake_install_deps fmt/a \ \ install_gtest' scripts/setup-centos8.sh

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

function process_script {
    sed -i 's/^  ninja -C "${BINARY_DIR}" install/  sudo ninja -C "${BINARY_DIR}" install/g' scripts/setup-helper-functions.sh
    sed -i "s/-mavx2 -mfma -mavx -mf16c -mlzcnt -std=c++17/-march=${BUILD_MARCH} -std=c++17 -mno-avx512f -mno-avx512vl -mno-avx512cd -mno-avx512dq -mno-avx512bw/g" scripts/setup-helper-functions.sh
    # make sure developer passed valid value and correct
    if [[ "$BUILD_TYPE" == "debug" ]] || [[ "$BUILD_TYPE" == "Debug" ]] || [[ "$BUILD_TYPE" == "DEBUG" ]]; then
      BUILD_TYPE='debug';
    else
      BUILD_TYPE='release';
    fi
    if [[ "$LINUX_DISTRIBUTION" != "ubuntu" && "$LINUX_DISTRIBUTION" != "debian" ]]; then
      process_setup_centos8
    fi
}

function compile {
    TARGET_BUILD_COMMIT=$(git rev-parse --verify HEAD)
    if [[ "$LINUX_DISTRIBUTION" == "ubuntu" || "$LINUX_DISTRIBUTION" == "debian" ]]; then
      NPROC=$BUILD_NPROC install_velox_deps_ubuntu
    else # Assume CentOS
      NPROC=$BUILD_NPROC scripts/setup-centos8.sh
    fi
    EXTRA_CMAKE_FLAGS="${EXTRA_CMAKE_FLAGS} -DVELOX_ENABLE_PARQUET=ON -DVELOX_BUILD_TESTING=OFF -DVELOX_ENABLE_DUCKDB=OFF -DVELOX_BUILD_TEST_UTILS=ON"
    EXTRA_CMAKE_FLAGS="${EXTRA_CMAKE_FLAGS} -DVELOX_ENABLE_HDFS=${ENABLE_HDFS} -DVELOX_ENABLE_S3=${ENABLE_S3} -DCMAKE_BUILD_TYPE=${BUILD_TYPE} "
    EXTRA_CMAKE_FLAGS="${EXTRA_CMAKE_FLAGS} -DMAX_LINK_JOBS=${BUILD_MAX_LINK_JOBS} -DMAX_HIGH_MEM_JOBS=${BUILD_MAX_HIGH_MEM_JOBS}"
    echo "Using EXTRA_CMAKE_FLAGS=${EXTRA_CMAKE_FLAGS}"
    make $BUILD_TYPE EXTRA_CMAKE_FLAGS="${EXTRA_CMAKE_FLAGS}"
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


echo "Build Velox begin."
echo "CMAKE Arguments being used:"
echo "VELOX_HOME=${VELOX_HOME}"
echo "ENABLE_S3=${ENABLE_S3}"
echo "ENABLE_HDFS=${ENABLE_HDFS}"
echo "BUILD_PROTOBUF=${BUILD_PROTOBUF}"
echo "BUILD_TYPE=${BUILD_TYPE}"

if [ ! -d $VELOX_HOME ]; then
  echo "$VELOX_HOME does not exist. Exiting."
  exit 1
fi

cd $VELOX_HOME
check_commit
process_script
compile

echo "Velox Installation finished."

