#!/bin/bash

BASEDIR=$(dirname $0)
source "$BASEDIR/builddeps-veloxbe.sh"

cd $GLUTEN_DIR
mvn clean package -Pbackends-velox -Pceleborn -Puniffle -Pspark-3.2 -DskipTests
mvn clean package -Pbackends-velox -Pceleborn -Puniffle -Pspark-3.3 -DskipTests
mvn clean package -Pbackends-velox -Pceleborn -Puniffle -Pspark-3.4 -DskipTests
mvn clean package -Pbackends-velox -Pceleborn -Puniffle -Pspark-3.5 -DskipTests
