#!/bin/bash

set -ex

BASEDIR=$(readlink -f $(dirname $0))

TIMESTAMP=$(date +%s)

export EXTRA_DOCKER_OPTIONS="--name buildhere-veloxbe-dev-$TIMESTAMP --detach -v $BASEDIR/scripts:/opt/scripts"
export PRESERVE_CONTAINER=ON

$BASEDIR/../../cbash.sh 'sleep infinity'
docker exec buildhere-veloxbe-dev-$TIMESTAMP bash -c '/opt/scripts/all.sh'
docker exec buildhere-veloxbe-dev-$TIMESTAMP bash -c 'service ssh restart'

