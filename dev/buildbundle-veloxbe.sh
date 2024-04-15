#!/bin/bash

BASEDIR=$(dirname $0)
source "$BASEDIR/builddeps-veloxbe.sh"

cd $GLUTEN_DIR

DISTDIR="$GLUTEN_DIR/dist"
rm -rf "$DISTDIR"
mkdir -p "$DISTDIR"

mvn clean package -Pbackends-velox -Prss -Pspark-3.2 -DskipTests
cp package/target/gluten-*-bundle-spark*.jar "$DISTDIR"

mvn clean package -Pbackends-velox -Prss -Pspark-3.3 -DskipTests
cp package/target/gluten-*-bundle-spark*.jar "$DISTDIR"

mvn clean package -Pbackends-velox -Prss -Pspark-3.4 -DskipTests
cp package/target/gluten-*-bundle-spark*.jar "$DISTDIR"

mvn clean package -Pbackends-velox -Prss -Pspark-3.5 -DskipTests
cp package/target/gluten-*-bundle-spark*.jar "$DISTDIR"
