#!/bin/bash

set -exu

#setting gluten root path
GLUTEN_DIR=/opt/gluten
BUILD_TYPE=release
NPROC=$(nproc --ignore=2)
ENABLE_EP_CACHE=OFF
ARROW_REPO=https://github.com/oap-project/arrow.git
ARROW_BRANCH=arrow-8.0.0-gluten-20220427a


for arg in "$@"
do
    case $arg in
        --gluten_dir=*)
        GLUTEN_DIR=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --build_dir=*)
        BUILD_DIR=("${arg#*=}")
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

BUILD_DIR="$GLUTEN_DIR/ep/build-arrow/build"
ARROW_SOURCE_DIR="${BUILD_DIR}/arrow_ep"
ARROW_INSTALL_DIR="${BUILD_DIR}/arrow_install"
TARGET_BUILD_COMMIT=""

function check_rp_cache {
  TARGET_BUILD_COMMIT="$(git ls-remote $ARROW_REPO $ARROW_BRANCH | awk '{print $1;}')"
  echo "Target Arrow commit: $TARGET_BUILD_COMMIT"
  if [ $ENABLE_EP_CACHE == "ON" ]; then
    if [ -e ${BUILD_DIR}/arrow-commit.cache ]; then
      LAST_BUILT_COMMIT="$(cat ${BUILD_DIR}/arrow-commit.cache)"
      if [ -n $LAST_BUILT_COMMIT ]; then
        if [ -z "$TARGET_BUILD_COMMIT" ]
          then
            echo "Unable to parse Arrow commit: $TARGET_BUILD_COMMIT."
            exit 1
            fi
            if [ "$TARGET_BUILD_COMMIT" = "$LAST_BUILT_COMMIT" ]; then
                echo "Arrow build of commit $TARGET_BUILD_COMMIT was cached, skipping build..."
                exit 0
            else
                echo "Found cached commit $LAST_BUILT_COMMIT for Arrow which is different with target commit $TARGET_BUILD_COMMIT."
            fi
        fi
    fi
  fi

  if [ -e ${BUILD_DIR}/arrow-commit.cache ]; then
      rm -f ${BUILD_DIR}/arrow-commit.cache
  fi
}

function incremental_build {
  if [ $ENABLE_EP_CACHE == "ON" ] && [ -d $ARROW_SOURCE_DIR ]; then
    echo "Applying incremental build for Arrow..."
    cd $ARROW_SOURCE_DIR
    git init .
    EXISTS=`git show-ref refs/heads/build_$TARGET_BUILD_COMMIT || true`
    if [ -z "$EXISTS" ]; then
      git fetch $ARROW_REPO $TARGET_BUILD_COMMIT:build_$TARGET_BUILD_COMMIT
    fi
    git reset --hard HEAD
    git checkout build_$TARGET_BUILD_COMMIT
  fi
}

check_rp_cache
echo "Building Arrow from Source for Gazelle..."
echo "CMAKE Arguments:"
echo "GLUTEN_DIR=${GLUTEN_DIR}"
echo "BUILD_DIR=${BUILD_DIR}"
echo "BUILD_TYPE=${BUILD_TYPE}"

if [ -d $ARROW_INSTALL_DIR ]; then
    rm -rf $ARROW_INSTALL_DIR
fi

mkdir -p $ARROW_INSTALL_DIR
incremental_build

pushd $ARROW_SOURCE_DIR
mkdir -p java/c/build
pushd java/c/build
cmake ..
cmake --build .
popd

cmake -DARROW_BUILD_STATIC=OFF \
        -DARROW_BUILD_SHARED=ON \
        -DARROW_COMPUTE=ON \
        -DARROW_SUBSTRAIT=ON \
        -DARROW_S3=ON \
        -DARROW_PARQUET=ON \
        -DARROW_ORC=OFF \
        -DARROW_HDFS=ON \
        -DARROW_BOOST_USE_SHARED=OFF \
        -DARROW_JNI=ON \
        -DARROW_DATASET=ON \
        -DARROW_WITH_PROTOBUF=ON \
        -DARROW_PROTOBUF_USE_SHARED=OFF \
        -DARROW_WITH_SNAPPY=ON \
        -DARROW_WITH_LZ4=ON \
        -DARROW_WITH_RE2=ON \
        -DARROW_FILESYSTEM=ON \
        -DARROW_JEMALLOC=ON \
        -DARROW_SIMD_LEVEL=AVX2 \
        -DARROW_RUNTIME_SIMD_LEVEL=MAX \
        -DARROW_DEPENDENCY_SOURCE=BUNDLED \
        -Dre2_SOURCE=BUNDLED \
        -DProtobuf_SOURCE=BUNDLED \
        -DCMAKE_INSTALL_PREFIX=$ARROW_INSTALL_DIR \
        -DCMAKE_INSTALL_LIBDIR=lib \
        cpp

make -j$NPROC

make install

cd java
mvn clean install -P arrow-jni -pl dataset,c -am -Darrow.cpp.build.dir=$ARROW_INSTALL_DIR/lib -DskipTests -Dcheckstyle.skip -Dmaven.gitcommitid.skip=true

echo "Successfully built Arrow from Source !!!"
echo $TARGET_BUILD_COMMIT > "${BUILD_DIR}/arrow-commit.cache"