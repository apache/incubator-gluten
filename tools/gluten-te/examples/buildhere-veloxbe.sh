#!/bin/bash

set -ex

BASEDIR=$(dirname $0)

EXTRA_MAVEN_OPTIONS="-Pspark-3.2 \
                     -Pbackends-velox \
                     -Dbuild_protobuf=ON \
                     -Dbuild_cpp=ON \
                     -Dbuild_arrow=ON \
                     -Dbuild_velox=ON \
                     -Dbuild_velox_from_source=ON \
                     -Dbuild_gazelle_cpp=OFF \
                     -DskipTests \
                     -Dscalastyle.skip=true \
                     -Dcheckstyle.skip=true"

$BASEDIR/../cmvn.sh clean install $EXTRA_MAVEN_OPTIONS
