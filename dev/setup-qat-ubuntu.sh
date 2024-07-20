#!/bin/bash

# Exit on any error
set -e

# Define versions and URLs
QAT_VERSION="QAT20.L.1.0.50-00003"
QAT_URL="https://downloadmirror.intel.com/783270/${QAT_VERSION}.tar.gz"
ZSTD_VERSION="zstd-1.5.5"
ZSTD_URL="https://github.com/facebook/zstd/releases/download/v1.5.5/${ZSTD_VERSION}.tar.gz"

# Install required packages for QAT
sudo apt-get update
sudo apt-get install -y zlib1g-dev libisal-dev libudev-dev udev yasm libboost-all-dev gcc g++ pkg-config linux-headers-$(uname -r)

# Download and extract QAT driver
sudo rm -rf /opt/QAT20
sudo mkdir -p /opt/QAT20
sudo wget -O /opt/QAT20/${QAT_VERSION}.tar.gz ${QAT_URL}
sudo tar -C /opt/QAT20 -zxf /opt/QAT20/${QAT_VERSION}.tar.gz

# Compile and install QAT driver
cd /opt/QAT20
sudo ./configure
sudo make
sudo make install

# Update environment variables for QAT driver
echo "export ICP_ROOT=/opt/QAT20" >> ~/.bashrc

# Download and extract zstd
sudo wget -O /opt/${ZSTD_VERSION}.tar.gz ${ZSTD_URL}
sudo tar -C /opt -zxf /opt/${ZSTD_VERSION}.tar.gz

# Compile and install zstd
sudo mkdir -p /opt/${ZSTD_VERSION}/build/cmake/build
cd /opt/${ZSTD_VERSION}/build/cmake/build
sudo cmake -DCMAKE_INSTALL_PREFIX=/usr/local ..
sudo make -j
sudo make install

echo -e "QAT setup is complete."
echo -e "To apply the changes, please log out and log back in."

