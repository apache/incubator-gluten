#!/bin/bash

set -exu

ARROW_REPO=https://github.com/apache/arrow.git
ARROW_BRANCH=apache-arrow-12.0.0
ARROW_HOME=""
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
  --arrow_home=*)
    ARROW_HOME=("${arg#*=}")
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
  if [ -d $ARROW_SOURCE_DIR ]; then
    echo "Arrow source folder $ARROW_SOURCE_DIR already exists..."
    cd $ARROW_SOURCE_DIR
    git init .
    EXISTS=$(git show-ref refs/tags/build_$TARGET_BUILD_COMMIT || true)
    if [ -z "$EXISTS" ]; then
      git fetch $ARROW_REPO $TARGET_BUILD_COMMIT:refs/tags/build_$TARGET_BUILD_COMMIT
    fi
    git reset --hard HEAD
    git checkout refs/tags/build_$TARGET_BUILD_COMMIT
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

if [ "$ARROW_HOME" == "" ]; then
  ARROW_HOME="$CURRENT_DIR/../build"
fi
ARROW_SOURCE_DIR="${ARROW_HOME}/arrow_ep"

checkout_code

# apply patches
git apply --reverse --check $CURRENT_DIR/memorypool.patch >/dev/null 2>&1 || git apply $CURRENT_DIR/memorypool.patch
# apply patch for custom codec
if [ $ENABLE_CUSTOM_CODEC == ON ]; then
  git apply --reverse --check $CURRENT_DIR/custom-codec.patch >/dev/null 2>&1 || git apply $CURRENT_DIR/custom-codec.patch
fi

cat > cpp/src/parquet/symbols.map <<EOF
{
  global:
    extern "C++" {
      *parquet::*;
    };
  local:
    *;
};
EOF

echo "Arrow-get finished."
