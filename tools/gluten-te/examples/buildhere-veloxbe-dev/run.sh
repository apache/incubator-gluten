#!/bin/bash

set -ex

BASEDIR=$(readlink -f $(dirname $0))

export EXTRA_DOCKER_OPTIONS="-v $BASEDIR/scripts:/opt/scripts"

$BASEDIR/../../cbash.sh 'bash -c /opt/scripts/all.sh'
