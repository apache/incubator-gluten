#!/bin/bash

set -exu

VELOX_REPO=https://github.com/zhouyuan/velox.git
VELOX_BRANCH=wip_bp_4276

#Set on run gluten on HDFS
ENABLE_HDFS=OFF
#It can be set to OFF when compiling velox again
BUILD_PROTOBUF=ON
#Set on run gluten on S3
ENABLE_S3=OFF

LINUX_DISTRIBUTION=$(. /etc/os-release && echo ${ID})

for arg in "$@"; do
  case $arg in
  --velox_repo=*)
    VELOX_REPO=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --velox_branch=*)
    VELOX_BRANCH=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --build_protobuf=*)
    BUILD_PROTOBUF=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --enable_hdfs=*)
    ENABLE_HDFS=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --enable_s3=*)
    ENABLE_S3=("${arg#*=}")
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
  sed -i '/^sudo --preserve-env apt update && sudo apt install -y/a\  *thrift* \\' scripts/setup-ubuntu.sh
  sed -i '/^sudo --preserve-env apt update && sudo apt install -y/a\  libiberty-dev \\' scripts/setup-ubuntu.sh
  sed -i '/^sudo --preserve-env apt update && sudo apt install -y/a\  libxml2-dev \\' scripts/setup-ubuntu.sh
  sed -i '/^sudo --preserve-env apt update && sudo apt install -y/a\  libkrb5-dev \\' scripts/setup-ubuntu.sh
  sed -i '/^sudo --preserve-env apt update && sudo apt install -y/a\  libgsasl7-dev \\' scripts/setup-ubuntu.sh
  sed -i '/^sudo --preserve-env apt update && sudo apt install -y/a\  libuuid1 \\' scripts/setup-ubuntu.sh
  sed -i '/^sudo --preserve-env apt update && sudo apt install -y/a\  uuid-dev \\' scripts/setup-ubuntu.sh
  sed -i '/^sudo --preserve-env apt update && sudo apt install -y/a\  libiberty-dev \\' scripts/setup-ubuntu.sh
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
  sed -i '/^cmake_install_deps gflags/i function install_gtest {\n  wget https://github.com/google/googletest/archive/refs/tags/release-1.12.1.tar.gz\n  tar -xzf release-1.12.1.tar.gz\n  cd googletest-release-1.12.1\n  mkdir -p build && cd build && cmake -DBUILD_GTEST=ON -DBUILD_GMOCK=ON -DINSTALL_GTEST=ON -DINSTALL_GMOCK=ON -DBUILD_SHARED_LIBS=ON ..\n  make "-j$(nproc)"\n  make install\n  cd ../../ && ldconfig\n}\n' scripts/setup-centos8.sh
  sed -i '/^cmake_install_deps gflags/i FB_OS_VERSION=v2022.11.14.00\nfunction install_folly {\n  github_checkout facebook/folly "${FB_OS_VERSION}"\n  cmake_install -DBUILD_TESTS=OFF\n}\n' scripts/setup-centos8.sh
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

echo "Preparing Velox source code..."
echo "ENABLE_HDFS=${ENABLE_HDFS}"
echo "BUILD_PROTOBUF=${BUILD_PROTOBUF}"

CURRENT_DIR=$(
  cd "$(dirname "$BASH_SOURCE")"
  pwd
)

# checkout code
VELOX_SOURCE_DIR="$CURRENT_DIR/../build/velox_ep"
TARGET_BUILD_COMMIT="$(git ls-remote $VELOX_REPO $VELOX_BRANCH | awk '{print $1;}')"
if [ -d $VELOX_SOURCE_DIR ]; then
  echo "Velox source folder $VELOX_SOURCE_DIR already exists..."
  cd $VELOX_SOURCE_DIR
  git init .
  EXISTS=$(git show-ref refs/heads/build_$TARGET_BUILD_COMMIT || true)
  if [ -z "$EXISTS" ]; then
    git fetch $VELOX_REPO $TARGET_BUILD_COMMIT:build_$TARGET_BUILD_COMMIT
  fi
  git reset --hard HEAD
  git checkout build_$TARGET_BUILD_COMMIT
else
  git clone $VELOX_REPO -b $VELOX_BRANCH $VELOX_SOURCE_DIR
  cd $VELOX_SOURCE_DIR
  git checkout $TARGET_BUILD_COMMIT
fi
#sync submodules
git submodule sync --recursive
git submodule update --init --recursive

# apply patches
sed -i 's/^  ninja -C "${BINARY_DIR}" install/  sudo ninja -C "${BINARY_DIR}" install/g' scripts/setup-helper-functions.sh
sed -i 's/-mavx2 -mfma -mavx -mf16c -mlzcnt -std=c++17/-march=native -std=c++17 -mno-avx512f/g' scripts/setup-helper-functions.sh
if [[ "$LINUX_DISTRIBUTION" == "ubuntu" || "$LINUX_DISTRIBUTION" == "debian" ]]; then
  process_setup_ubuntu
else # Assume CentOS
  process_setup_centos8
fi

echo "Velox-get finished."
