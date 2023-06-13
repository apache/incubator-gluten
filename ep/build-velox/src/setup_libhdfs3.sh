#! /bin/bash

set -ex

SCRIPT_DIR="$(realpath "$(dirname "$0")")"
SOURCE_DIR="/tmp/gluten-libhdfs3-$RANDOM"

mkdir "$SOURCE_DIR"
trap "rm -rf \$SOURCE_DIR" EXIT

git clone https://github.com/ClickHouse/libhdfs3.git "$SOURCE_DIR"
cd "$SOURCE_DIR"

git apply "$SCRIPT_DIR/libhdfs3.patch"
cmake -Bbuild
cmake --build build -j
sudo cmake --install build