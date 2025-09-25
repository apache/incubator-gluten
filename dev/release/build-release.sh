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

set -eu

source /opt/rh/devtoolset-11/enable
source /opt/rh/rh-git227/enable

CURRENT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
GLUTEN_HOME=${CURRENT_DIR}/../../
cd ${GLUTEN_HOME}

# Enable static build. Enable the support for S3, GCS, HDFS, and ABFS.
./dev/builddeps-veloxbe.sh --enable_vcpkg=ON --build_arrow=OFF --build_tests=OFF --build_benchmarks=OFF \
                           --build_examples=OFF --enable_s3=ON --enable_gcs=ON --enable_hdfs=ON --enable_abfs=ON

JAVA_VERSION=$("java" -version 2>&1 | awk -F '"' '/version/ {print $2}')

# Use JDK 8 as it is the most commonly used version.
if [[ $JAVA_VERSION == 1.8* ]]; then
  echo "JDK 8 is being used."
else
  echo "Error: JDK 8 is required. Current version is $JAVA_VERSION."
  exit 1
fi

# Build for officially supported Spark versions only.
# Enable all feature modules, including Celeborn, Uniffle, Iceberg, Delta, Hudi, and Paimon.
for spark_version in 3.2 3.3 3.4 3.5
do
  mvn clean install -Pbackends-velox -Pspark-${spark_version} -Pceleborn,uniffle -Piceberg,delta,hudi,paimon -DskipTests
done
