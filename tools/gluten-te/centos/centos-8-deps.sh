#! /bin/sh

set -ex

sed -i -e "s|mirrorlist=|#mirrorlist=|g" /etc/yum.repos.d/CentOS-* 
# The connection to vault.centos.org in CI is unstable
# sed -i -e "s|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g" /etc/yum.repos.d/CentOS-*
minorver=8.5.2111
sed -i -e \
  "s|^#baseurl=http://mirror.centos.org/\$contentdir/\$releasever|baseurl=https://mirrors.aliyun.com/centos-vault/$minorver|g" \
  /etc/yum.repos.d/CentOS-*.repo

dnf install -y epel-release sudo
yum -y update && yum clean all && yum install -y dnf-plugins-core
yum config-manager --set-enabled powertools
dnf --enablerepo=powertools install -y ninja-build
dnf --enablerepo=powertools install -y libdwarf-devel
dnf install -y --setopt=install_weak_deps=False ccache gcc-toolset-9 git wget which libevent-devel \
  openssl-devel re2-devel libzstd-devel lz4-devel double-conversion-devel \
  curl-devel cmake libicu-devel

yum -y update && yum clean all && yum install -y java-1.8.0-openjdk-devel
dnf -y install gcc-toolset-9-gcc gcc-toolset-9-gcc-c++