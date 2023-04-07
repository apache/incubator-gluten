#! /bin/sh
# shellcheck disable=SC1091,SC2155

if [ -f "/opt/rh/gcc-toolset-9/enable" ]; then
    . /opt/rh/gcc-toolset-9/enable
elif [ -f "/opt/rh/devtoolset-9/enable" ]; then # CentOS 7
    . /opt/rh/devtoolset-9/enable
fi

export MAKEFLAGS="-j$(nproc)"