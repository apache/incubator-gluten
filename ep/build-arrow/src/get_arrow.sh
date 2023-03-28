#!/bin/bash

set -exu
#setting gluten root path
ARROW_REPO=https://github.com/oap-project/arrow.git
#for velox_backend
ARROW_BRANCH=arrow-11.0.0-gluten
ENABLE_CUSTOM_CODEC=OFF

for arg in "$@"; do
  case $arg in
  --arrow_repo=*)
    ARROW_REPO=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --arrow_branch=*)
    ARROW_BRANCH=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --enable_custom_codec=*)
    ENABLE_CUSTOM_CODEC=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  *)
    OTHER_ARGUMENTS+=("$1")
    shift # Remove generic argument from processing
    ;;
  esac
done

function checkout_code {
  TARGET_BUILD_COMMIT="$(git ls-remote $ARROW_REPO $ARROW_BRANCH | awk '{print $1;}')"
  ARROW_SOURCE_DIR="$CURRENT_DIR/../build/arrow_ep"
  if [ -d $ARROW_SOURCE_DIR ]; then
    echo "Arrow source folder $ARROW_SOURCE_DIR already exists..."
    cd $ARROW_SOURCE_DIR
    git init .
    EXISTS=$(git show-ref refs/heads/build_$TARGET_BUILD_COMMIT || true)
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

echo "Preparing Arrow source code..."
echo "ENABLE_CUSTOM_CODEC=${ENABLE_CUSTOM_CODEC}"

CURRENT_DIR=$(
  cd "$(dirname "$BASH_SOURCE")"
  pwd
)

checkout_code

# apply patches
git apply --reverse --check $CURRENT_DIR/memorypool.patch >/dev/null 2>&1 || git apply $CURRENT_DIR/memorypool.patch
# apply patch for custom codec
if [ $ENABLE_CUSTOM_CODEC == ON ]; then
  git apply --reverse --check $CURRENT_DIR/custom-codec.patch >/dev/null 2>&1 || git apply $CURRENT_DIR/custom-codec.patch
fi

echo "Arrow-get finished."
