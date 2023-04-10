#! /bin/sh

set -ex

yum -y install epel-release centos-release-scl
yum -y install \
    git \
    dnf \
    cmake3 \
    devtoolset-9 \
    java-1.8.0-openjdk \
    java-1.8.0-openjdk-devel \
    ninja-build \
    wget

echo fastestmirror=True >> /etc/dnf/dnf.conf
ln -s /usr/bin/cmake3 /usr/local/bin/cmake