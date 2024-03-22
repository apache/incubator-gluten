#! /bin/bash

if [ -z "${BASH_SOURCE[0]}" ] || [ "$0" == "${BASH_SOURCE[0]}" ]; then
    echo "env.sh should only be sourced in bash" >&2
    exit 1
fi

SCRIPT_ROOT="$(realpath "$(dirname "${BASH_SOURCE[0]}")")"
init_vcpkg_env=$("${SCRIPT_ROOT}/init.sh")
eval "$init_vcpkg_env"
