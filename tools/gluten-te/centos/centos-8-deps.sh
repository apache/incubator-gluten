#! /bin/sh

set -ex

sed -i -e "s|mirrorlist=|#mirrorlist=|g" /etc/yum.repos.d/CentOS-* 
# The connection to vault.centos.org in CI is unstable
sed -i -e "s|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g" /etc/yum.repos.d/CentOS-*
# minorver=8.5.2111
# sed -i -e \
#   "s|^#baseurl=http://mirror.centos.org/\$contentdir/\$releasever|baseurl=https://mirrors.aliyun.com/centos-vault/$minorver|g" \
#   /etc/yum.repos.d/CentOS-*.repo

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