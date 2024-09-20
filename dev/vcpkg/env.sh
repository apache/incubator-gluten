#! /bin/bash
set -e

exec 3>&1 >&2

if [ -z "${BASH_SOURCE[0]}" ] || [ "$0" == "${BASH_SOURCE[0]}" ]; then
    echo "env.sh should only be sourced in bash" >&2
    exit 1
fi

SCRIPT_ROOT="$(realpath "$(dirname "${BASH_SOURCE[0]}")")"
init_vcpkg_env=$("${SCRIPT_ROOT}/init.sh" $@)
#init_vcpkg_env="${SCRIPT_ROOT}/init.sh $@"
eval "$init_vcpkg_env"
#source ${SCRIPT_ROOT}/init.sh $@

export VCPKG_ROOT="$SCRIPT_ROOT/.vcpkg"
export VCPKG="$SCRIPT_ROOT/.vcpkg/vcpkg"
export VCPKG_TRIPLET=x64-linux-avx
export VCPKG_TRIPLET_INSTALL_DIR=${SCRIPT_ROOT}/vcpkg_installed/${VCPKG_TRIPLET}
export EXPORT_TOOLS_PATH="${VCPKG_TRIPLET_INSTALL_DIR}/tools/protobuf"

#cat <<EOF >&3
if [ "\${GLUTEN_VCPKG_ENABLED:-}" != "${VCPKG_ROOT}" ]; then
    export VCPKG_ROOT=${VCPKG_ROOT}
    export VCPKG_MANIFEST_DIR=${SCRIPT_ROOT}
    export VCPKG_TRIPLET=${VCPKG_TRIPLET}

    export CMAKE_TOOLCHAIN_FILE=${SCRIPT_ROOT}/toolchain.cmake
    export PKG_CONFIG_PATH=${VCPKG_TRIPLET_INSTALL_DIR}/lib/pkgconfig:${VCPKG_TRIPLET_INSTALL_DIR}/share/pkgconfig:\${PKG_CONFIG_PATH:-}
    export PATH="${EXPORT_TOOLS_PATH}:$PATH"

    export GLUTEN_VCPKG_ENABLED=${VCPKG_ROOT}
else
    echo "Gluten's vcpkg environment is enabled" >&2
fi
#EOF
