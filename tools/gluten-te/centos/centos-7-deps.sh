#! /bin/sh

set -ex

# The connection to centos.org in CI is unstable
# curl -o /etc/yum.repos.d/CentOS-Base.repo https://mirrors.aliyun.com/repo/Centos-7.repo

yum -y install epel-release centos-release-scl
yum -y install \
    cmake3 \
    devtoolset-9 \
    java-1.8.0-openjdk \
    java-1.8.0-openjdk-devel \
    ninja-build \
    wget \
    perl-IPC-Cmd \
    autoconf \
    automake \
    libtool \
    gettext-devel \
    bison

# Install git 2.x
yum -y install https://packages.endpointdev.com/rhel/7/os/x86_64/endpoint-repo.x86_64.rpm
yum -y install git

# Install flex 2.6.4
FLEX_URL="https://github.com/westes/flex/releases/download/v2.6.4/flex-2.6.4.tar.gz"
mkdir -p /tmp/flex
wget -q --max-redirect 3 -O - "${FLEX_URL}" | tar -xz -C /tmp/flex --strip-components=1
cd /tmp/flex
./autogen.sh
./configure
make install
cd
rm -rf /tmp/flex

ln -s /usr/bin/cmake3 /usr/local/bin/cmake

# Required by velox setup scripts
yum -y install dnf