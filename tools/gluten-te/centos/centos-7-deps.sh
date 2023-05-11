#! /bin/sh

set -ex

CENTOS_MIRROR_URL=https://mirrors.edge.kernel.org/centos
CENTOS_MIRROR_GPGKEY="${CENTOS_MIRROR_URL}/RPM-GPG-KEY-CentOS-7"

cp /etc/yum.repos.d/CentOS-Base.repo /tmp/CentOS-Base.repo
sed -i "/^mirrorlist/d;s/^\#baseurl=/baseurl=/" /tmp/CentOS-Base.repo
sed -i "s|^gpgkey=.*$|gpgkey=${CENTOS_MIRROR_GPGKEY}|" /tmp/CentOS-Base.repo
sed -i "s|http://mirror.centos.org/centos|${CENTOS_MIRROR_URL}|" /tmp/CentOS-Base.repo
rm /etc/yum.repos.d/*
mv /tmp/CentOS-Base.repo /etc/yum.repos.d/

# Disable fastestmirror
sed -i "s/enabled=1/enabled=0/" /etc/yum/pluginconf.d/fastestmirror.conf 

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

ln -s /usr/bin/cmake3 /usr/local/bin/cmake
