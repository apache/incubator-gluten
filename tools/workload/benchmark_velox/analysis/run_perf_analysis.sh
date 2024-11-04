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

SCRIPT_LOCATION=$(dirname $0)
PAUS=$HOME/PAUS

while [[ $# -gt 0 ]]; do
  case $1 in
    --ts)
      TS="$2"
      shift # past argument
      shift # past value
      ;;
    --base-dir)
      BASEDIR="$2"
      shift # past argument
      shift # past value
      ;;
    --name)
      NAME="$2"
      shift # past argument
      shift # past value
      ;;
    --appid)
      APPID="$2"
      shift # past argument
      shift # past value
      ;;
    --disk)
      DISK="$2"
      shift # past argument
      shift # past value
      ;;
    --nic)
      NIC="$2"
      shift # past argument
      shift # past value
      ;;
    --tz)
      SPARK_TZ="$2"
      shift # past argument
      shift # past value
      ;;
    --comp-appid)
      COMP_APPID="$2"
      shift # past argument
      shift # past value
      ;;
    --comp-base-dir)
      COMP_BASEDIR="$2"
      shift # past argument
      shift # past value
      ;;
    --comp-name)
      COMP_NAME="$2"
      shift # past argument
      shift # past value
      ;;
    *)
      echo "Error: Unknown argument: $1"
      exit 1
      ;;
  esac
done

# Validation: Check if any of the required variables are empty
if [[ -z "${TS+x}" || -z "${BASEDIR+x}" || -z "${NAME+x}" || -z "${APPID+x}" || -z "${DISK+x}" || -z "${NIC+x}" || -z "${SPARK_TZ+x}" ]]; then
  echo "Error: One or more required arguments are missing or empty."
  exit 1
fi

mkdir -p $PAUS
if [ ! -f "$PAUS/perf_analysis_template.ipynb" ]; then
  cp $SCRIPT_LOCATION/perf_analysis_template.ipynb $PAUS/
fi
if [ ! -f "$PAUS/sparklog.ipynb" ]; then
  cp $SCRIPT_LOCATION/sparklog.ipynb $PAUS/
fi

mkdir -p $PAUS/$BASEDIR
cd $PAUS/$BASEDIR
mkdir -p html

nb_name0=${TS}_${NAME}_${APPID}
nb_name=${nb_name0}.ipynb

cp -f $PAUS/perf_analysis_template.ipynb $nb_name
hadoop fs -mkdir -p /history
hadoop fs -cp -f /$BASEDIR/$APPID/app.log /history/$APPID

EXTRA_ARGS=""
if [ -v COMP_APPID ]
then
  if [[ -z "${COMP_BASEDIR+x}" || -z "${COMP_NAME+x}" ]]; then
	echo "Missing --comp-base-dir or --comp-name"
	exit 1
  fi
  hadoop fs -cp -f /$COMP_BASEDIR/$COMP_APPID/app.log /history/$COMP_APPID
  EXTRA_ARGS="--compare_appid $COMP_APPID --compare_basedir $COMP_BASEDIR --compare_name $COMP_NAME"
fi

source ~/paus-env/bin/activate

python3 $SCRIPT_LOCATION/run.py --inputnb $nb_name --outputnb ${nb_name0}.nbconvert.ipynb --appid $APPID --disk $DISK --nic $NIC --tz $SPARK_TZ --basedir $BASEDIR --name $NAME $EXTRA_ARGS

jupyter nbconvert --to html  --no-input ./${nb_name0}.nbconvert.ipynb --output html/${nb_name0}.html --template classic > /dev/null 2>&1
