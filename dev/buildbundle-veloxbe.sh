#!/bin/bash

BASEDIR=$(dirname $0)
source "$BASEDIR/builddeps-veloxbe.sh"

function build_for_spark {
  spark_version=$1
  mvn clean package -Pbackends-velox -Pceleborn -Puniffle -Pspark-$spark_version -DskipTests
}

cd $GLUTEN_DIR

# SPARK_VERSION is defined in builddeps-veloxbe.sh
if [ "$SPARK_VERSION" = "ALL" ]; then
  for spark_version in 3.2 3.3 3.4 3.5
  do
    build_for_spark $spark_version
  done
else
  build_for_spark $SPARK_VERSION
fi
