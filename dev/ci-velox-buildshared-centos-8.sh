#!/bin/bash

set -e

# TODO: will be removed after docker is updated.
dnf install -y --setopt=install_weak_deps=False gcc-toolset-11
source /opt/rh/gcc-toolset-11/enable
./dev/builddeps-veloxbe.sh --run_setup_script=OFF --build_arrow=OFF --enable_ep_cache=OFF --build_tests=ON \
    --build_examples=ON --build_benchmarks=ON
