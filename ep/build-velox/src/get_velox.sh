#!/bin/bash

set -exu

#setting this dir if need
GLUTEN_DIR=/opt/gluten
VELOX_REPO=https://github.com/marin-ma/velox-oap.git
VELOX_BRANCH=cast-ut

for arg in "$@"
do
    case $arg in
        --gluten_dir=*)
        GLUTEN_DIR=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
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
echo "Velox Get start..."

mkdir -p "$GLUTEN_DIR/ep/build-velox/build"

VELOX_SOURCE_DIR="$GLUTEN_DIR/ep/build-velox/build/velox_ep"

if [ -d $VELOX_SOURCE_DIR ]; then
    rm -rf $VELOX_SOURCE_DIR
fi

git clone $VELOX_REPO -b $VELOX_BRANCH $VELOX_SOURCE_DIR

cd $VELOX_SOURCE_DIR

TARGET_BUILD_COMMIT="$(git ls-remote $VELOX_REPO $VELOX_BRANCH | awk '{print $1;}')"
echo "Target Velox commit: $TARGET_BUILD_COMMIT"

git checkout $TARGET_BUILD_COMMIT

#sync submodules
git submodule sync --recursive
git submodule update --init --recursive

echo "Velox get finished."

