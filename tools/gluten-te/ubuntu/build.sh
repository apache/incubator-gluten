#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -ex

BASEDIR=$(dirname $0)

source "$BASEDIR/buildenv.sh"

## Debug build flags

# Create debug build
DEBUG_BUILD=${DEBUG_BUILD:-$DEFAULT_DEBUG_BUILD}

if [ -n $JDK_DEBUG_BUILD ]
then
  echo "Do not set JDK_DEBUG_BUILD manually!"
fi

if [ -n $GLUTEN_DEBUG_BUILD ]
then
  echo "Do not set GLUTEN_DEBUG_BUILD manually!"
fi

if [ "$DEBUG_BUILD" == "ON" ]
then
  JDK_DEBUG_BUILD=OFF
  GLUTEN_DEBUG_BUILD=ON
else
  JDK_DEBUG_BUILD=OFF
  GLUTEN_DEBUG_BUILD=OFF
fi

# The target branches
TARGET_GLUTEN_REPO=${TARGET_GLUTEN_REPO:-$DEFAULT_GLUTEN_REPO}
TARGET_GLUTEN_BRANCH=${TARGET_GLUTEN_BRANCH:-$DEFAULT_GLUTEN_BRANCH}

# The branches used to prepare dependencies
CACHE_GLUTEN_REPO=${CACHE_GLUTEN_REPO:-$DEFAULT_GLUTEN_REPO}
CACHE_GLUTEN_BRANCH=${CACHE_GLUTEN_BRANCH:-$DEFAULT_GLUTEN_BRANCH}

# Backend type
BUILD_BACKEND_TYPE=${BUILD_BACKEND_TYPE:-$DEFAULT_BUILD_BACKEND_TYPE}

# Build will result in this image
DOCKER_TARGET_IMAGE_BUILD=${DOCKER_TARGET_IMAGE_BUILD:-$DEFAULT_DOCKER_TARGET_IMAGE_BUILD}

DOCKER_TARGET_IMAGE_BUILD_WITH_OS_IMAGE="$DOCKER_TARGET_IMAGE_BUILD-$OS_IMAGE"

## Fetch target commit

TARGET_GLUTEN_COMMIT="$(git ls-remote $TARGET_GLUTEN_REPO $TARGET_GLUTEN_BRANCH | awk '{print $1;}')"

if [ -z "$TARGET_GLUTEN_COMMIT" ]
then
  echo "Unable to parse TARGET_GLUTEN_COMMIT."
  exit 1
fi

##

BUILD_DOCKER_BUILD_ARGS=

BUILD_DOCKER_BUILD_ARGS="$BUILD_DOCKER_BUILD_ARGS --ulimit nofile=8192:8192"
BUILD_DOCKER_BUILD_ARGS="$BUILD_DOCKER_BUILD_ARGS --build-arg DOCKER_TARGET_IMAGE_BUILDENV_WITH_OS_IMAGE=$DOCKER_TARGET_IMAGE_BUILDENV_WITH_OS_IMAGE"
BUILD_DOCKER_BUILD_ARGS="$BUILD_DOCKER_BUILD_ARGS --build-arg JDK_DEBUG_BUILD=$JDK_DEBUG_BUILD"
BUILD_DOCKER_BUILD_ARGS="$BUILD_DOCKER_BUILD_ARGS --build-arg GLUTEN_DEBUG_BUILD=$GLUTEN_DEBUG_BUILD"
BUILD_DOCKER_BUILD_ARGS="$BUILD_DOCKER_BUILD_ARGS --build-arg TARGET_GLUTEN_REPO=$TARGET_GLUTEN_REPO"
BUILD_DOCKER_BUILD_ARGS="$BUILD_DOCKER_BUILD_ARGS --build-arg TARGET_GLUTEN_COMMIT=$TARGET_GLUTEN_COMMIT"
BUILD_DOCKER_BUILD_ARGS="$BUILD_DOCKER_BUILD_ARGS --build-arg CACHE_GLUTEN_REPO=$CACHE_GLUTEN_REPO"
BUILD_DOCKER_BUILD_ARGS="$BUILD_DOCKER_BUILD_ARGS --build-arg CACHE_GLUTEN_BRANCH=$CACHE_GLUTEN_BRANCH"
BUILD_DOCKER_BUILD_ARGS="$BUILD_DOCKER_BUILD_ARGS --build-arg BUILD_BACKEND_TYPE=$BUILD_BACKEND_TYPE"
BUILD_DOCKER_BUILD_ARGS="$BUILD_DOCKER_BUILD_ARGS -f $BASEDIR/dockerfile-build"
BUILD_DOCKER_BUILD_ARGS="$BUILD_DOCKER_BUILD_ARGS --target gluten-build"
BUILD_DOCKER_BUILD_ARGS="$BUILD_DOCKER_BUILD_ARGS -t $DOCKER_TARGET_IMAGE_BUILD_WITH_OS_IMAGE"

if [ -n "$DOCKER_CACHE_IMAGE" ]
then
  BUILD_DOCKER_BUILD_ARGS="$BUILD_DOCKER_BUILD_ARGS --cache-from $DOCKER_CACHE_IMAGE"
fi

BUILD_DOCKER_BUILD_ARGS="$BUILD_DOCKER_BUILD_ARGS $BASEDIR"

docker build $BUILD_DOCKER_BUILD_ARGS

# EOF
