#!/bin/bash

set -eu

NPROC=$(nproc)
TESTS=OFF
BUILD_ARROW=OFF
STATIC_ARROW=OFF
ARROW_ROOT=/usr/local
# option gazelle_cpp
BACKEND_TYPE=velox
ENABLE_EP_CACHE=OFF

for arg in "$@"
do
    case $arg in
        -t=*|--tests=*)
        TESTS=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        -a=*|--build_arrow=*)
        BUILD_ARROW=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        -s=*|--static_arrow=*)
        STATIC_ARROW=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        -ar=*|--arrow_root=*)
        ARROW_ROOT=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        -b=*|--backend_type=*)
        BACKEND_TYPE=("${arg#*=}")
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

function compile_velox_arrow {
    pushd $ARROW_SOURCE_DIR
    mkdir -p java/build
    pushd java/build
    cmake \
        -DCMAKE_INSTALL_PREFIX=$ARROW_INSTALL_DIR/lib \
        ..
    cmake --build . --target install
    popd

    mkdir -p cpp/build
    pushd cpp/build
    cmake -G Ninja \
            -DARROW_BUILD_STATIC=OFF \
            -DARROW_COMPUTE=ON \
            -DARROW_CSV=ON \
            -DARROW_DATASET=ON \
            -DARROW_FILESYSTEM=ON \
            -DARROW_JSON=ON \
            -DARROW_PARQUET=ON \
            -DARROW_SUBSTRAIT=ON \
            -DARROW_WITH_BROTLI=ON \
            -DARROW_WITH_BZ2=ON \
            -DARROW_WITH_LZ4=ON \
            -DARROW_WITH_RE2=ON \
            -DARROW_WITH_SNAPPY=ON \
            -DARROW_WITH_UTF8PROC=ON \
            -DARROW_WITH_ZLIB=ON \
            -DARROW_WITH_ZSTD=ON \
            -DCMAKE_BUILD_TYPE=Release \
            -DARROW_BUILD_SHARED=ON \
            -DARROW_SUBSTRAIT=ON \
            -DARROW_S3=ON \
            -DARROW_GANDIVA_JAVA=ON \
            -DARROW_GANDIVA=ON \
            -DARROW_ORC=OFF \
            -DARROW_HDFS=ON \
            -DARROW_BOOST_USE_SHARED=OFF \
            -DARROW_JNI=ON \
            -DARROW_WITH_PROTOBUF=ON \
            -DARROW_PROTOBUF_USE_SHARED=OFF \
            -DARROW_FLIGHT=OFF \
            -DARROW_JEMALLOC=ON \
            -DARROW_SIMD_LEVEL=AVX2 \
            -DARROW_RUNTIME_SIMD_LEVEL=MAX \
            -DARROW_DEPENDENCY_SOURCE=BUNDLED \
            -Dre2_SOURCE=AUTO \
            -DCMAKE_INSTALL_PREFIX=$ARROW_INSTALL_DIR \
            -DCMAKE_INSTALL_LIBDIR=lib \
            ..
    cmake --build . --target install
    popd

    cd java
    mvn clean install -P arrow-jni -pl dataset,gandiva,c -am -Darrow.cpp.build.dir=$ARROW_INSTALL_DIR/lib -DskipTests -Dcheckstyle.skip \
        -Darrow.c.jni.dist.dir=$ARROW_INSTALL_DIR/lib -Dmaven.gitcommitid.skip=true
}

function compile_gazelle_arrow {
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
            -DARROW_GANDIVA_JAVA=ON \
            -DARROW_GANDIVA=ON \
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
            -DARROW_WITH_ZSTD=OFF \
            -DARROW_WITH_BROTLI=OFF \
            -DARROW_WITH_ZLIB=OFF \
            -DARROW_WITH_FASTPFOR=OFF \
            -DARROW_FILESYSTEM=ON \
            -DARROW_JSON=ON \
            -DARROW_CSV=ON \
            -DARROW_FLIGHT=OFF \
            -DARROW_JEMALLOC=ON \
            -DARROW_SIMD_LEVEL=AVX2 \
            -DARROW_RUNTIME_SIMD_LEVEL=MAX \
            -DARROW_DEPENDENCY_SOURCE=BUNDLED \
            -Dre2_SOURCE=AUTO \
            -DCMAKE_INSTALL_PREFIX=$ARROW_INSTALL_DIR \
            -DCMAKE_INSTALL_LIBDIR=lib \
            cpp

    make -j$NPROC

    make install

    cd java
    mvn clean install -P arrow-jni -pl dataset,gandiva,c -am -Darrow.cpp.build.dir=$ARROW_INSTALL_DIR/lib -DskipTests -Dcheckstyle.skip -Dmaven.gitcommitid.skip=true
}

echo "CMAKE Arguments:"
echo "TESTS=${TESTS}"
echo "BUILD_ARROW=${BUILD_ARROW}"
echo "STATIC_ARROW=${STATIC_ARROW}"
echo "ARROW_ROOT=${ARROW_ROOT}"

CURRENT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
echo $CURRENT_DIR

cd ${CURRENT_DIR}

if [ $BUILD_ARROW == "ON" ]; then
  mkdir -p build
  ARROW_REPO=https://github.com/oap-project/arrow.git

  if [ $BACKEND_TYPE == "velox" ]; then
      ARROW_BRANCH=backend_velox_main
  elif [ $BACKEND_TYPE == "gazelle_cpp" ]; then
      ARROW_BRANCH=arrow-8.0.0-gluten-20220427a
  else
      echo "Unrecognizable backend type: $BACKEND_TYPE."
      exit 1
  fi

  TARGET_BUILD_COMMIT="$(git ls-remote $ARROW_REPO $ARROW_BRANCH | awk '{print $1;}')"
  echo "Target Arrow commit: $TARGET_BUILD_COMMIT"
  if [ $ENABLE_EP_CACHE == "ON" ]; then
    if [ -e ${CURRENT_DIR}/build/arrow-commit.cache ]; then
        LAST_BUILT_COMMIT="$(cat ${CURRENT_DIR}/build/arrow-commit.cache)"
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

  if [ -e ${CURRENT_DIR}/build/arrow-commit.cache ]; then
      rm -f ${CURRENT_DIR}/build/arrow-commit.cache
  fi

  echo "Building Arrow from Source ..."
  ARROW_PREFIX="${CURRENT_DIR}/build" # Use build directory as ARROW_PREFIX
  ARROW_SOURCE_DIR="${ARROW_PREFIX}/arrow_ep"
  ARROW_INSTALL_DIR="${ARROW_PREFIX}/arrow_install"

  echo "ARROW_PREFIX=${ARROW_PREFIX}"
  echo "ARROW_SOURCE_DIR=${ARROW_SOURCE_DIR}"

  mkdir -p $ARROW_ROOT

  if [ -d $ARROW_INSTALL_DIR ]; then
      rm -rf $ARROW_INSTALL_DIR
  fi

  if [ $ENABLE_EP_CACHE == "ON" ] && [ -d $ARROW_SOURCE_DIR ]; then
    echo "Applying incremental build for Arrow..."
    pushd $ARROW_SOURCE_DIR
    git init .
    EXISTS=`git show-ref refs/heads/build_$TARGET_BUILD_COMMIT || true`
    if [ -z "$EXISTS" ]; then
      git fetch $ARROW_REPO $TARGET_BUILD_COMMIT:build_$TARGET_BUILD_COMMIT
    fi
    git reset --hard HEAD
    git checkout build_$TARGET_BUILD_COMMIT
  else
    echo "Creating brand-new build for Arrow..."
    if [ -d $ARROW_SOURCE_DIR ]; then
      rm -rf $ARROW_SOURCE_DIR
    fi
    git clone $ARROW_REPO -b $ARROW_BRANCH $ARROW_SOURCE_DIR
    pushd $ARROW_SOURCE_DIR
    git checkout $TARGET_BUILD_COMMIT
  fi

  if [ $BACKEND_TYPE == "velox" ]; then
    compile_velox_arrow
  elif [ $BACKEND_TYPE == "gazelle_cpp" ]; then
    compile_gazelle_arrow
  else
    echo "Unrecognizable backend type: $BACKEND_TYPE."
    exit 1
  fi

  echo "Successfully built Arrow from Source !!!"
  echo $TARGET_BUILD_COMMIT > "${CURRENT_DIR}/build/arrow-commit.cache"
else
  echo "Use ARROW_ROOT as Arrow Library Path"
  echo "ARROW_ROOT=${ARROW_ROOT}"
fi
