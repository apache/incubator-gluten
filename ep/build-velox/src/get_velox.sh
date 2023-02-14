#!/bin/bash

set -exu

VELOX_REPO=https://github.com/liujiayi771/velox.git
VELOX_BRANCH=stddev_pop

for arg in "$@"
do
    case $arg in
        --velox_repo=*)
        VELOX_REPO=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --velox_branch=*)
        VELOX_BRANCH=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        *)
        OTHER_ARGUMENTS+=("$1")
        shift # Remove generic argument from processing
        ;;
    esac
done

function check_ep_cache {
  TARGET_BUILD_COMMIT="$(git ls-remote $VELOX_REPO $VELOX_BRANCH | awk '{print $1;}')"
  echo "Target Velox commit: $TARGET_BUILD_COMMIT"
  if [ -f ${BUILD_DIR}/velox-commit.cache ]; then
    LAST_BUILT_COMMIT="$(cat ${BUILD_DIR}/velox-commit.cache)"
    if [ -n $LAST_BUILT_COMMIT ]; then
      if [ -z "$TARGET_BUILD_COMMIT" ]
        then
          echo "Unable to parse Velox commit: $TARGET_BUILD_COMMIT."
          exit 1
          fi
          if [ "$TARGET_BUILD_COMMIT" = "$LAST_BUILT_COMMIT" ]; then
              echo "Velox build of commit $TARGET_BUILD_COMMIT was cached."
              exit 0
          else
              echo "Found cached commit $LAST_BUILT_COMMIT for Velox which is different with target commit $TARGET_BUILD_COMMIT."
          fi
      fi
  fi
}

function checkout_code {
  if [ -d $VELOX_SOURCE_DIR ]; then
    echo "Applying incremental build for Velox..."
    cd $VELOX_SOURCE_DIR
    git init .
    EXISTS=`git show-ref refs/heads/build_$TARGET_BUILD_COMMIT || true`
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
}

echo "Velox-get start..."
CURRENT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
mkdir -p "$CURRENT_DIR/../build"

BUILD_DIR="$CURRENT_DIR/../build"
VELOX_SOURCE_DIR="$CURRENT_DIR/../build/velox_ep"

check_ep_cache

if [ -f ${BUILD_DIR}/velox-commit.cache ]; then
    rm -f ${BUILD_DIR}/velox-commit.cache
fi

checkout_code

echo "Velox-get finished."

