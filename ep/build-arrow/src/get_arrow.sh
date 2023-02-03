#!/bin/bash

set -exu
#setting gluten root path
ARROW_REPO=https://github.com/oap-project/arrow.git
#for velox_backend
ARROW_BRANCH=backend_velox_main
#for gazelle backend
#ARROW_BRANCH=arrow-8.0.0-gluten-20220427a


for arg in "$@"
do
    case $arg in
        --arrow_repo=*)
        ARROW_REPO=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --arrow_branch=*)
        ARROW_BRANCH=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
	    *)
	    OTHER_ARGUMENTS+=("$1")
        shift # Remove generic argument from processing
        ;;
    esac
done

function check_ep_cache {
  TARGET_BUILD_COMMIT="$(git ls-remote $ARROW_REPO $ARROW_BRANCH | awk '{print $1;}')"
  echo "Target Arrow commit: $TARGET_BUILD_COMMIT"
  if [ -f ${ARROW_HOME}/arrow-commit.cache ]; then
    LAST_BUILT_COMMIT="$(cat ${ARROW_HOME}/arrow-commit.cache)"
    if [ -n $LAST_BUILT_COMMIT ]; then
      if [ -z "$TARGET_BUILD_COMMIT" ]
        then
          echo "Unable to parse Arrow commit: $TARGET_BUILD_COMMIT."
          exit 1
          fi
          if [ "$TARGET_BUILD_COMMIT" = "$LAST_BUILT_COMMIT" ]; then
              echo "Arrow build of commit $TARGET_BUILD_COMMIT was cached."
              exit 0
          else
              echo "Found cached commit $LAST_BUILT_COMMIT for Arrow which is different with target commit $TARGET_BUILD_COMMIT."
          fi
      fi
  fi
}

function checkout_code {
  if [ -d $ARROW_SOURCE_DIR ]; then
    echo "Applying incremental build for Arrow..."
    cd $ARROW_SOURCE_DIR
    git init .
    EXISTS=`git show-ref refs/heads/build_$TARGET_BUILD_COMMIT || true`
    if [ -z "$EXISTS" ]; then
      git fetch $ARROW_REPO $TARGET_BUILD_COMMIT:build_$TARGET_BUILD_COMMIT
    fi
    git reset --hard HEAD
    git checkout build_$TARGET_BUILD_COMMIT
  else
    git clone $ARROW_REPO -b $ARROW_BRANCH $ARROW_SOURCE_DIR
    cd $ARROW_SOURCE_DIR
    git checkout $TARGET_BUILD_COMMIT
  fi
}

echo "Arrow-get start..."
CURRENT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
mkdir -p "$CURRENT_DIR/../build"

ARROW_HOME="$CURRENT_DIR/../build/"
ARROW_SOURCE_DIR="$CURRENT_DIR/../build/arrow_ep"

check_ep_cache

if [ -f ${ARROW_HOME}/arrow-commit.cache ]; then
    rm -f ${ARROW_HOME}/arrow-commit.cache
fi

checkout_code

echo "Arrow-get finished."
