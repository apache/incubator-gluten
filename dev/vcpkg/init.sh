#! /bin/bash

set -e

exec 3>&1 >&2

BUILD_TESTS=OFF
ENABLE_S3=OFF
ENABLE_GCS=OFF
ENABLE_HDFS=OFF
ENABLE_ABFS=OFF

for arg in "$@"; do
  case $arg in
  --build_tests=*)
    BUILD_TESTS=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --enable_s3=*)
    ENABLE_S3=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --enable_gcs=*)
    ENABLE_GCS=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --enable_hdfs=*)
    ENABLE_HDFS=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --enable_abfs=*)
    ENABLE_ABFS=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  *)
    echo "Unrecognized argument: $arg"
    exit 1
    ;;
  esac
done

export SCRIPT_ROOT="$(realpath "$(dirname "$0")")"
export VCPKG_ROOT="$SCRIPT_ROOT/.vcpkg"
export VCPKG="$SCRIPT_ROOT/.vcpkg/vcpkg"
VCPKG_TRIPLET=x64-linux-avx

cd "$SCRIPT_ROOT"

if [ ! -d "$VCPKG_ROOT" ] || [ -z "$(ls "$VCPKG_ROOT")" ]; then
    git clone https://github.com/microsoft/vcpkg.git --branch 2023.10.19 "$VCPKG_ROOT"
fi
[ -f "$VCPKG" ] || "$VCPKG_ROOT/bootstrap-vcpkg.sh" -disableMetrics

sed -i "s/3.27.1/3.28.3/g" $VCPKG_ROOT/scripts/vcpkgTools.xml
sed -i "s/192374a68e2971f04974a194645726196d9b8ee7abd650d1e6f65f7aa2ccc9b186c3edb473bb4958c764532edcdd42f4182ee1fcb86b17d78b0bcd6305ce3df1/bd311ca835ef0914952f21d70d1753564d58de2ede02e80ede96e78cd2f40b4189e006007643ebb37792e13edd97eb4a33810bc8aca1eab6dd428eaffe1d2e38/g" $VCPKG_ROOT/scripts/vcpkgTools.xml

EXTRA_FEATURES=""
if [ "$BUILD_TESTS" = "ON" ]; then
  EXTRA_FEATURES+="--x-feature=duckdb "
fi
if [ "$ENABLE_S3" = "ON" ]; then
  EXTRA_FEATURES+="--x-feature=velox-s3 "
fi
if [ "$ENABLE_GCS" = "ON" ]; then
  EXTRA_FEATURES+="--x-feature=velox-gcs "
fi
if [ "$ENABLE_HDFS" = "ON" ]; then
  EXTRA_FEATURES+="--x-feature=velox-hdfs "
fi
if [ "$ENABLE_ABFS" = "ON" ]; then
  EXTRA_FEATURES+="--x-feature=velox-abfs"
fi


$VCPKG install --no-print-usage \
    --triplet="${VCPKG_TRIPLET}" --host-triplet="${VCPKG_TRIPLET}" ${EXTRA_FEATURES}

export VCPKG_TRIPLET_INSTALL_DIR=${SCRIPT_ROOT}/vcpkg_installed/${VCPKG_TRIPLET}
EXPORT_TOOLS_PATH="${VCPKG_TRIPLET_INSTALL_DIR}/tools/protobuf"

# This scripts depends on environment $CMAKE_TOOLCHAIN_FILE, which requires
# cmake >= 3.21. If system cmake < 3.25, vcpkg will download latest cmake. We
# can use vcpkg's internal cmake if we find it.
VCPKG_CMAKE_BIN_DIR=$(echo "${VCPKG_ROOT}"/downloads/tools/cmake-*/cmake-*/bin)
if [ -f "$VCPKG_CMAKE_BIN_DIR/cmake" ]; then
    EXPORT_TOOLS_PATH="${VCPKG_CMAKE_BIN_DIR}:${EXPORT_TOOLS_PATH}"
fi

export EXPORT_TOOLS_PATH=${EXPORT_TOOLS_PATH/%:/}

# For fixing a build error like below when gluten's build type is Debug:
# No rule to make target '/root/gluten/dev/vcpkg/vcpkg_installed/x64-linux-avx/debug/lib/libz.a',
# needed by 'releases/libvelox.so'
mkdir -p $VCPKG_TRIPLET_INSTALL_DIR/debug/lib/
cp $VCPKG_TRIPLET_INSTALL_DIR/lib/libz.a $VCPKG_TRIPLET_INSTALL_DIR/debug/lib
cp $VCPKG_TRIPLET_INSTALL_DIR/lib/libssl.a $VCPKG_TRIPLET_INSTALL_DIR/debug/lib
cp $VCPKG_TRIPLET_INSTALL_DIR/lib/libcrypto.a $VCPKG_TRIPLET_INSTALL_DIR/debug/lib
cp $VCPKG_TRIPLET_INSTALL_DIR/lib/liblzma.a $VCPKG_TRIPLET_INSTALL_DIR/debug/lib
cp $VCPKG_TRIPLET_INSTALL_DIR/lib/libdwarf.a $VCPKG_TRIPLET_INSTALL_DIR/debug/lib
# Allow libhdfs3.a is not installed as build option may not enable hdfs.
cp $VCPKG_TRIPLET_INSTALL_DIR/lib/libhdfs3.a $VCPKG_TRIPLET_INSTALL_DIR/debug/lib || true

