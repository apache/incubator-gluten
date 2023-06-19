#!/bin/bash

set -eu

GLUTEN_IT_JVM_ARGS=${GLUTEN_IT_JVM_ARGS:-"-Xmx2G -XX:ErrorFile=/var/log/java/hs_err_pid%p.log"}

BASEDIR=$(dirname $0)

BUILD_DIR=$BASEDIR/../target
JAR_PATH=$BUILD_DIR/gluten-it-1.0-SNAPSHOT-jar-with-dependencies.jar

if [[ ! -e $JAR_PATH ]]; then
  echo "Please build gluten-it first. For example: mvn clean package"
  exit 1
fi

java \
    --add-opens=java.base/java.lang=ALL-UNNAMED \
    --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
    --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
    --add-opens=java.base/java.io=ALL-UNNAMED \
    --add-opens=java.base/java.net=ALL-UNNAMED \
    --add-opens=java.base/java.nio=ALL-UNNAMED \
    --add-opens=java.base/java.util=ALL-UNNAMED \
    --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
    --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
    --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
    --add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
    --add-opens=java.base/sun.security.action=ALL-UNNAMED \
    --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
    --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
   --illegal-access=permit -Dio.netty.tryReflectionSetAccessible=true  $GLUTEN_IT_JVM_ARGS -cp $JAR_PATH io.glutenproject.integration.tpc.Tpc $@
