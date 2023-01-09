#!/bin/bash
####################################################################################################
#  The main function of this script is to allow developers to build the environment with one click #
#  Recommended commands for first-time installation:                                               #
#  ./dev/buildbundle-veloxbe.sh                                                            #
####################################################################################################
set -exu

CURRENT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
GLUTEN_DIR="$CURRENT_DIR/.."
BUILD_TYPE=release
BUILD_TESTS=OFF
BUILD_BENCHMARKS=OFF
BUILD_JEMALLOC=ON
ENABLE_HBM=OFF
BUILD_PROTOBUF=ON
ENABLE_S3=OFF
ENABLE_HDFS=OFF
ENABLE_EP_CACHE=OFF
for arg in "$@"
do
    case $arg in
        --build_type=*)
        BUILD_TYPE=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --build_test=*)
        BUILD_TESTS=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --build_benchmarks=*)
        BUILD_BENCHMARKS=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --build_jemalloc=*)
        BUILD_JEMALLOC=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --enable_hbm=*)
        ENABLE_HBM=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --build_protobuf=*)
        BUILD_PROTOBUF=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --enable_s3=*)
        ENABLE_S3=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --enable_hdfs=*)
        ENABLE_HDFS=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --build_arrow_from_source=*)
        BUILD_ARROW_FROM_SOURCE=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --build_velox_from_source=*)
        BUILD_VELOX_FROM_SOURCE=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        --enable_ep_cache=*)
        ENABLE_EP_CACHE=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
	      *)
        OTHER_ARGUMENTS+=("$1")
        shift # Remove generic argument from processing
        ;;
    esac
done

##install arrow
cd $GLUTEN_DIR/ep/build-arrow/src
./get_arrow.sh --enable_ep_cache=$ENABLE_EP_CACHE

if [ $ENABLE_EP_CACHE == 'OFF' ] || [ ! -f $GLUTEN_DIR/ep/build-arrow/build/arrow-commit.cache ]; then
  ./build_arrow_for_velox.sh --build_type=$BUILD_TYPE --build_test=$BUILD_TESTS --build_benchmarks=$BUILD_BENCHMARKS \
                           --enable_ep_cache=$ENABLE_EP_CACHE
fi

##install velox
cd $GLUTEN_DIR/ep/build-velox/src
./get_velox.sh --enable_ep_cache=$ENABLE_EP_CACHE

if [ $ENABLE_EP_CACHE == 'OFF' ] || [ ! -f $GLUTEN_DIR/ep/build-velox/build/velox-commit.cache ]; then
  ./build_velox.sh --build_protobuf=$BUILD_PROTOBUF --enable_s3=$ENABLE_S3 \
                 --build_type=$BUILD_TYPE --enable_hdfs=$ENABLE_HDFS  --build_type=$BUILD_TYPE \
                 --enable_ep_cache=$ENABLE_EP_CACHE
fi

## compile gluten cpp
cd $GLUTEN_DIR/cpp
./compile.sh --build_velox_backend=ON --build_type=$BUILD_TYPE --build_velox_backend=ON \
             --build_test=$BUILD_TESTS --build_benchmarks=$BUILD_BENCHMARKS --build_jemalloc=$BUILD_JEMALLOC \
             --enable_hbm=$ENABLE_HBM --enable_s3=$ENABLE_S3 --enable_hdfs=$ENABLE_HDFS

cd $GLUTEN_DIR
mvn clean package -Pbackends-velox -Pspark-3.2 -DskipTests
mvn clean package -Pbackends-velox -Pspark-3.3 -DskipTests


