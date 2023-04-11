#! /bin/sh

set -ex

# The connection to centos.org in CI is unstable
curl -o /etc/yum.repos.d/CentOS-Base.repo https://mirrors.aliyun.com/repo/Centos-7.repo

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
