#! /bin/bash

set -ex

SCRIPT_ROOT="$(realpath "$(dirname "$0")")"

if [ -z "$1" ]; then
    cat << EOF >&2
Export vcpkg manifest dir.

usage: $0 output_dir
EOF
    exit 1
fi

OUTPUT_DIR="$1"
[ -d "$OUTPUT_DIR" ] || mkdir -p "$OUTPUT_DIR"

rsync -a --delete \
    --exclude=/.vcpkg \
    --exclude=/vcpkg_installed \
    "$SCRIPT_ROOT/" "${OUTPUT_DIR/%\//}"