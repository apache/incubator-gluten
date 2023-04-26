#! /bin/sh

set -ex

sed -i "s/^enabled=0$/enable=1/" /etc/yum.repos.d/CentOS-Linux-PowerTools.repo
yum -y install \
    git \
    which \
    cmake \
    gcc-toolset-9-gcc \
    gcc-toolset-9-gcc-c++ \
    java-1.8.0-openjdk \
    java-1.8.0-openjdk-devel \
    ninja-build \
    wget \
    perl-IPC-Cmd \
    autoconf \
    automake \
    flex \
    bison \
    curl \
    zip \
    unzip \
    tar \
    python3