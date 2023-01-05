#!/bin/bash

set -ex

BASEDIR=$(dirname $0)

EXTRA_MAVEN_OPTIONS="-Pspark-3.2 \
                     -Pbackends-velox 
                     -DskipTests \
                     -Dscalastyle.skip=true \
                     -Dcheckstyle.skip=true"

$BASEDIR/../cbash.sh bash -c "dev/builddeps-veloxbe.sh && mvn clean install $EXTRA_MAVEN_OPTIONS"
