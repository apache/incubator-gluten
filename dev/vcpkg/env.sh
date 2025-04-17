#! /bin/bash
set -e

if [ -z "${BASH_SOURCE[0]}" ] || [ "$0" == "${BASH_SOURCE[0]}" ]; then
    echo "env.sh should only be sourced in bash" >&2
    exit 1
fi

SCRIPT_ROOT="$(realpath "$(dirname "${BASH_SOURCE[0]}")")"

export VCPKG_ROOT="$SCRIPT_ROOT/.vcpkg"
export VCPKG="$SCRIPT_ROOT/.vcpkg/vcpkg"
export VCPKG_TRIPLET=$([ "${CPU_TARGET:-}" = "aarch64" ] && echo "arm64-linux-neon" || echo "x64-linux-avx")
export VCPKG_TRIPLET_INSTALL_DIR=${SCRIPT_ROOT}/vcpkg_installed/${VCPKG_TRIPLET}

${SCRIPT_ROOT}/init.sh "$@"

if [ "${GLUTEN_VCPKG_ENABLED:-}" != "${VCPKG_ROOT}" ]; then
    EXPORT_TOOLS_PATH="${VCPKG_TRIPLET_INSTALL_DIR}/tools/protobuf"
    # The scripts depends on environment $CMAKE_TOOLCHAIN_FILE, which requires
    # cmake >= 3.21. If system cmake < 3.25, vcpkg will download latest cmake. We
    # can use vcpkg's internal cmake if we find it.
    VCPKG_CMAKE_BIN_DIR=$(echo "${VCPKG_ROOT}"/downloads/tools/cmake-*/cmake-*/bin)
    if [ -f "$VCPKG_CMAKE_BIN_DIR/cmake" ]; then
        EXPORT_TOOLS_PATH="${VCPKG_CMAKE_BIN_DIR}:${EXPORT_TOOLS_PATH}"
    fi
    EXPORT_TOOLS_PATH=${EXPORT_TOOLS_PATH/%:/}

    export VCPKG_ROOT=${VCPKG_ROOT}
    export VCPKG_MANIFEST_DIR=${SCRIPT_ROOT}
    export VCPKG_TRIPLET=${VCPKG_TRIPLET}

    export CMAKE_TOOLCHAIN_FILE=${SCRIPT_ROOT}/toolchain.cmake
    export PKG_CONFIG_PATH=${VCPKG_TRIPLET_INSTALL_DIR}/lib/pkgconfig:${VCPKG_TRIPLET_INSTALL_DIR}/share/pkgconfig:${PKG_CONFIG_PATH:-}
    export PATH="${EXPORT_TOOLS_PATH}:$PATH"

    export GLUTEN_VCPKG_ENABLED=${VCPKG_ROOT}
else
    echo "Gluten's vcpkg environment is already enabled" >&2
fi
