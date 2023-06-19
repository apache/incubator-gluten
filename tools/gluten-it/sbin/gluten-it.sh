#!/bin/bash

set -euf

GLUTEN_IT_JVM_ARGS=${GLUTEN_IT_JVM_ARGS:-"-Xmx2G -XX:ErrorFile=/var/log/java/hs_err_pid%p.log"}

BASEDIR=$(dirname $0)

LIB_DIR=$BASEDIR/../dist/target/lib
if [[ ! -d $LIB_DIR ]]; then
  echo "Lib directory not found at $LIB_DIR. Please build gluten-it first. For example: mvn clean install"
  exit 1
fi

JAR_PATH=$LIB_DIR/*

java $GLUTEN_IT_JVM_ARGS -cp $JAR_PATH io.glutenproject.integration.tpc.Tpc $@
