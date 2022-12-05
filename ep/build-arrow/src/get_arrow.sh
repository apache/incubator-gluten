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

echo "Arrow Get start..."
CURRENT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
mkdir -p "$CURRENT_DIR/../build"

ARROW_SOURCE_DIR="$CURRENT_DIR/../build/arrow_ep"

if [ -d $ARROW_SOURCE_DIR ]; then
    rm -rf $ARROW_SOURCE_DIR
fi

git clone $ARROW_REPO -b $ARROW_BRANCH $ARROW_SOURCE_DIR

cd $ARROW_SOURCE_DIR

TARGET_BUILD_COMMIT="$(git ls-remote $ARROW_REPO $ARROW_BRANCH | awk '{print $1;}')"
echo "Target Arrow commit: $TARGET_BUILD_COMMIT"

git checkout $TARGET_BUILD_COMMIT
