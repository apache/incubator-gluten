#! /bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -eu

PAPERMILL_ARGS=()
OUTPUT_DIR=$PWD

while [[ $# -gt 0 ]]; do
  case $1 in
    --notebook)
      NOTEBOOK="$2"
      shift # past argument
      shift # past value
      ;;
    --output-dir)
      OUTPUT_DIR="$2"
      shift # past argument
      shift # past value
      ;;
    --output-name)
      OUTPUT_NAME="$2"
      shift # past argument
      shift # past value
      ;;
    *)
      PAPERMILL_ARGS+=("$1") # save positional arg
      shift # past argument
      ;;
  esac
done

if [ -z ${NOTEBOOK+x} ]; then
  echo "Usage: $0 --notebook NOTEBOOK [--output-dir OUTPUT_DIR] [--output-name OUTPUT_NAME] [PAPERMILL_ARGS]"
  exit 0
fi


BASEDIR=$(dirname $0)
echo "Script location: ${BASEDIR}"

nbname=$(basename $NOTEBOOK .ipynb)

if [ -z ${OUTPUT_NAME+x} ]; then output_name=$nbname; else output_name=$(basename $OUTPUT_NAME .ipynb); fi

output_dir=$(realpath $OUTPUT_DIR)
mkdir -p $output_dir

rename_append_appid() {
  output_name=$1
  orig_nb=$2

  output_appid=`grep "appid: " $orig_nb | awk -F' ' '{print $2}' | sed 's/....$//'`
  if [ -n "$output_appid" ];
  then
    rename_nb=${output_dir}/${output_name}-${output_appid}.ipynb
    echo "Rename notebook $orig_nb to $rename_nb"
    mv $orig_nb $rename_nb
  fi
}

run() {
  output_name=${output_name}-$(date +"%H%M%S")
  output_nb=${output_dir}/${output_name}.ipynb
  papermill --inject-output-path $NOTEBOOK \
    ${PAPERMILL_ARGS[@]} \
    $output_nb
  rename_append_appid $output_name $output_nb
}

run

