#!/bin/bash

set -ex

CURRENT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
GLUTEN_DIR="$CURRENT_DIR/.."

cd "$GLUTEN_DIR"
source ./dev/vcpkg/env.sh
./dev/buildbundle-veloxbe.sh --enable_s3=ON --enable_hdfs=ON --enable_gcs=ON --arrow_binary=/opt/gluten/dev/vcpkg/
mvn clean package -Pbackends-velox -Prss -Pspark-3.2 -DskipTests
mvn clean package -Pbackends-velox -Prss -Pspark-3.3 -DskipTests
mvn clean package -Pbackends-velox -Prss -Pspark-3.4 -DskipTests
mvn clean package -Pbackends-velox -Prss -Pspark-3.5 -DskipTests
