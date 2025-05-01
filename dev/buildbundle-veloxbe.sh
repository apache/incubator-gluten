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

BASEDIR=$(dirname $0)
source "$BASEDIR/builddeps-veloxbe.sh"

function build_for_spark {
  spark_version=$1
  mvn clean package -Pbackends-velox -Pspark-$spark_version -DskipTests
}

function check_supported {
  PLATFORM=$(mvn help:evaluate -Dexpression=platform -q -DforceStdout)
  ARCH=$(mvn help:evaluate -Dexpression=arch -q -DforceStdout)
  if [ "$PLATFORM" == "null object or invalid expression" ] || [ "$ARCH" == "null object or invalid expression" ]; then
    OS_NAME=$(mvn help:evaluate -Dexpression=os.name -q -DforceStdout)
    OS_ARCH=$(mvn help:evaluate -Dexpression=os.arch -q -DforceStdout)
    echo "$OS_NAME-$OS_ARCH is not supported by current Gluten build."
    exit 1
  fi
}

cd $GLUTEN_DIR

check_supported

# SPARK_VERSION is defined in builddeps-veloxbe.sh
if [ "$SPARK_VERSION" = "ALL" ]; then
  for spark_version in 3.2 3.3 3.4 3.5
  do
    build_for_spark $spark_version
  done
else
  build_for_spark $SPARK_VERSION
fi
