#!/bin/bash
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

: '
Note: please use data-generate-vxx.sh for parquet files preparation, then use this script for files scan.
It will try reading each file and backup it as well as log if errors(spark-failure, gluten-failure, result
mismatch, fallback) happen
'
parquet_version=XXX
log_dir="/path/to/log/dir/"
traverse_files() {
    local dir="$1"
    for file in "$dir"/*; do
	sed -i "/val fileName/d" parquet-read.scala
	sed -i "24 a\val fileName: String =\"$file\"" ./parquet-read.scala
	date_time=$(date +%Y%m%d%H)
	file_name=$(basename "$file")
	# store output
	sh parquet-read.sh > ${file_name}_${date_time}_log 2>&1
	error_code=$?
	date_time=$(date +%Y%m%d%H)
	target_folder=""
	if [ ${error_code} = 3 ];then
	    cat ${file_name}_${date_time}_log|grep -rn "org.apache.gluten.exception.GlutenException: java.lang.RuntimeException"
	    error_type=$?
	    if [ ${error_type} = 0 ];then
	        target_folder="gluten-failure"
            else
		target_folder="spark-failure"
	    fi
	elif [ ${error_code} = 2 ];then
	    target_folder="mismatch"
	elif [ ${error_code} = 1 ];then
	    target_folder="fallback"
	fi
	if [ ${error_code} != 0 ];then
	    echo "!!!!Copying data and log to ${log_dir}/${parquet_version}/${target_folder}/${date_time}"
	    mkdir -p ${log_dir}/${parquet_version}/${target_folder}/${date_time}/
	    cp ${file} ${log_dir}/${parquet_version}/${target_folder}/$date_time/${file_name}_${date_time}
	    mv ${file_name}_${date_time}_log ${log_dir}/${parquet_version}/${target_folder}/${date_time}
	fi
    done
}

traverse_files "/path/to/folder/of/generated/data/"

