#!/bin/bash

# ref: https://clickhouse.com/docs/en/development/build

echo "Install Prerequisites"
sudo apt-get install git cmake ccache python3 ninja-build nasm yasm gawk lsb-release wget software-properties-common gnupg

echo "Install and Use the Clang compiler"
sudo bash -c "$(wget -O - https://apt.llvm.org/llvm.sh)"

echo "add CC and CXX to .bashrc"
echo "export CC=clang-16" >> ~/.bashrc
echo "export CXX=clang++-16" >> ~/.bashrc
source ~/.bashrc