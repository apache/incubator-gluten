#!/bin/bash

echo "Install cmake and ninja"
apt install -y git cmake ccache python3 ninja-build yasm gawk
echo "Install llvm"
LLVM_VERSION=15
export LLVM_PUBKEY_HASH="bda960a8da687a275a2078d43c111d66b1c6a893a3275271beedf266c1ff4a0cdecb429c7a5cccf9f486ea7aa43fd27f"
wget -nv -O /tmp/llvm-snapshot.gpg.key https://apt.llvm.org/llvm-snapshot.gpg.key
echo "${LLVM_PUBKEY_HASH} /tmp/llvm-snapshot.gpg.key" | sha384sum -c
apt-key add /tmp/llvm-snapshot.gpg.key
export CODENAME="$(lsb_release --codename --short | tr 'A-Z' 'a-z')"
echo "deb [trusted=yes] http://apt.llvm.org/${CODENAME}/ llvm-toolchain-${CODENAME}-${LLVM_VERSION} main" >> /etc/apt/sources.list

apt install -y llvm-${LLVM_VERSION} clang-${LLVM_VERSION}
