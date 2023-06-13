#!/bin/bash

set -ex

BASEDIR=$(readlink -f $(dirname $0))

TIMESTAMP=$(date +%s)

export EXTRA_DOCKER_OPTIONS="--name buildhere-veloxbe-dev-$TIMESTAMP --detach -v $BASEDIR/scripts:/opt/scripts"
export PRESERVE_CONTAINER=ON

$BASEDIR/../../cbash.sh 'bash /root/.cmd.sh'
docker exec buildhere-veloxbe-dev-$TIMESTAMP '/opt/scripts/all.sh'
