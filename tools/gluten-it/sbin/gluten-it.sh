#!/bin/bash

set -eu

BASEDIR=$(dirname $0)

BUILD_DIR=$BASEDIR/../target
JAR_PATH=$BUILD_DIR/gluten-it-1.0-SNAPSHOT-jar-with-dependencies.jar

if [[ ! -e $JAR_PATH ]]; then
  echo "Please build gluten-it first. For example: mvn clean package"
  exit 1
fi

java -Xmx24G -XX:ErrorFile=/var/log/java/hs_err_pid%p.log -cp $JAR_PATH io.glutenproject.integration.tpc.Tpc $@
