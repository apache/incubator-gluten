#!/bin/bash

set -eux

GLUTEN_VERSION=0.5.0-SNAPSHOT
GLUTEN_IT_VERSION=1.0-SNAPSHOT

GLUTEN_IT_JVM_ARGS=${GLUTEN_IT_JVM_ARGS:-"-Xmx2G -XX:ErrorFile=/var/log/java/hs_err_pid%p.log"}

BASEDIR=$(dirname $0)

BUILD_DIR=$BASEDIR/../target

GLUTEN_JAR_PATH=~/.m2/repository/io/glutenproject/gluten-package/$GLUTEN_VERSION/gluten-package-$GLUTEN_VERSION.jar
if [[ ! -e $GLUTEN_JAR_PATH ]]; then
  echo "Please build and install gluten first."
  exit 1
fi

GLUTEN_IT_JAR_PATH=$BUILD_DIR/gluten-it-$GLUTEN_IT_VERSION-jar-with-dependencies.jar
if [[ ! -e $GLUTEN_IT_JAR_PATH ]]; then
  echo "Please build gluten-it first. For example: mvn clean package"
  exit 1
fi

JAR_PATH=
JAR_PATH=$JAR_PATH:$GLUTEN_JAR_PATH
JAR_PATH=$JAR_PATH:$GLUTEN_IT_JAR_PATH

java $GLUTEN_IT_JVM_ARGS -cp $JAR_PATH io.glutenproject.integration.tpc.Tpc $@
