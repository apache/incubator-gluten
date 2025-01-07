#!/bin/bash

BASEDIR=$(dirname $0)
source "$BASEDIR/builddeps-veloxbe.sh"

function build_for_spark {
  spark_version=$1
  mvn clean package -Pbackends-velox -Pceleborn -Puniffle -Pspark-$spark_version -DskipTests
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
