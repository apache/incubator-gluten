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

opts=$(getopt -o c:g:b:o:h --long clickhouse:,gluten:,build:,output:,help -- "$@")

eval set -- "$opts"

while true; do
    case "$1" in
        -g | --gluten )
            GLUTEN_DIR=$2
            shift 2
            ;;
        -c | --clickhouse )
            CLICKHOUSE_DIR=$2
            shift 2
            ;;
        -b | --build )
            BUILD_DIR=$2
            shift 2
            ;;
        -o | --output )
            OUTPUT_DIR=$2
            shift
            ;;
        -h | --help )
            echo "Usage: build.sh -g <gluten_root_dir> -c <clickhouse_root_dir> [-b <build_dir>] [-o <output_dir>]"
            shift
            ;;
        -- )
            shift
            break
            ;;
        * )
            break;
            ;;
    esac
done

if [ -z "$GLUTEN_DIR" ]; then
    echo "Miss gluten source root directory"
    exit 1
fi

if [ -z "$CLICKHOUSE_DIR" ]; then
    echo "Miss clickhouse source root directory"
    exit 1
fi

CURRENT_DIR=$(pwd)
if [ -z "$BUILD_DIR" ]; then
    mkdir -p build
    BUILD_DIR=${CURRENT_DIR}/build
    echo "Will use ${BUILD_DIR} as the build directory" 
fi

if [ -z "$OUTPUT_DIR" ]; then
    mkdir -p output
    OUTPUT_DIR=${CURRENT_DIR}/build
    echo "Will use ${OUTPUT_DIR} as the output directory" 
fi


docker run  --rm \
    -v $GLUTEN_DIR:/gluten \
    -v $CLICKHOUSE_DIR:/clickhouse \
    -v /tmp/.cache:/ccache \
    -v $BUILD_DIR:/build \
    -v $OUTPUT_DIR:/output \
    libch_builder
