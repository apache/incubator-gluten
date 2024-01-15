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

BASEDIR=$(readlink -f $(dirname $0))

source "$BASEDIR/../../defaults.conf"

if [ -z "$GITHUB_RUN_ID" ]
then
  echo "Unable to parse GITHUB_RUN_ID."
  exit 1
fi

if [ -z "$GITHUB_JOB" ]
then
  echo "Unable to parse GITHUB_JOB."
  exit 1
fi

if [ -z "$GITHUB_SHA" ]
then
  echo "Unable to parse GITHUB_SHA."
  exit 1
fi

export EXTRA_DOCKER_OPTIONS="$EXTRA_DOCKER_OPTIONS --name gha-checkout-$GITHUB_JOB-$GITHUB_RUN_ID --detach -v $BASEDIR/scripts:/opt/scripts"
export NON_INTERACTIVE=ON

$BASEDIR/../../cbash-build.sh 'sleep 14400'

# The target branches
TARGET_GLUTEN_REPO=${TARGET_GLUTEN_REPO:-$DEFAULT_GLUTEN_REPO}

$BASEDIR/exec.sh "/opt/scripts/init.sh $TARGET_GLUTEN_REPO $GITHUB_SHA"
