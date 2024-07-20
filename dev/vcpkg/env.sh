#!/bin/sh

if [ -n "$BASH_VERSION" ]; then
    # Replace `$0` with `BASH_SOURCE[0]` in Bash because it returns the shell when using `source`.
    SCRIPT_PATH=${BASH_SOURCE[0]}
else
    SCRIPT_PATH="$0"
fi

# source command does not start a new process, so the cmdline would not contains current script name.
CMD=$(cat /proc/$$/cmdline)
if [[ ${CMD} == *"$SCRIPT_PATH"* ]]; then
    echo "env.sh should only be sourced" >&2
    exit 1
fi

SCRIPT_ROOT="$(realpath "$(dirname "$SCRIPT_PATH")")"
init_vcpkg_env=$("${SCRIPT_ROOT}/init.sh")
eval "$init_vcpkg_env"
