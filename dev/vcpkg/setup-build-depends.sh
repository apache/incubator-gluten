#! /bin/bash

set -e

## Install functions begin

install_centos_any_maven() {
    if [ -z "$(which mvn)" ]; then
        maven_install_dir=/opt/maven-3.6.3
        if [ -d /opt/maven-3.6.3 ]; then
            echo "Failed to install maven: ${maven_install_dir} is exists" >&2
            exit 1
        fi

        cd /tmp
        wget https://downloads.apache.org/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.tar.gz
        tar -xvf apache-maven-3.6.3-bin.tar.gz
        rm apache-maven-3.6.3-bin.tar.gz
        mv apache-maven-3.6.3 "${maven_install_dir}"
        ln -s "${maven_install_dir}/bin/mvn" /usr/local/bin/mvn
    fi
}

install_centos_7() {
    yum -y install epel-release centos-release-scl
    yum -y install \
        wget curl tar zip unzip which \
        cmake3 ninja-build perl-IPC-Cmd autoconf automake libtool \
        devtoolset-9 \
        bison \
        java-1.8.0-openjdk java-1.8.0-openjdk-devel

    # git>2.7.4
    if [[ "$(git --version)" != "git version 2."* ]]; then
        [ -f /etc/yum.repos.d/ius.repo ] || yum -y install https://repo.ius.io/ius-release-el7.rpm
        yum -y remove git
        yum -y install git236
    fi

    # flex>=2.6.0
    if [[ "$(PATH="/usr/local/bin:$PATH" flex --version 2>&1)" != "flex 2.6."* ]]; then
        yum -y install gettext-devel
        FLEX_URL="https://github.com/westes/flex/releases/download/v2.6.4/flex-2.6.4.tar.gz"
        mkdir -p /tmp/flex
        wget -q --max-redirect 3 -O - "${FLEX_URL}" | tar -xz -C /tmp/flex --strip-components=1
        cd /tmp/flex
        ./autogen.sh
        ./configure
        make install
        cd
        rm -rf /tmp/flex
    fi

    install_centos_any_maven
}

install_centos_8() {
    yum -y install \
        wget curl tar zip unzip git which \
        cmake ninja-build perl-IPC-Cmd autoconf automake libtool \
        gcc-toolset-9-gcc gcc-toolset-9-gcc-c++ \
        flex bison python3 \
        java-1.8.0-openjdk java-1.8.0-openjdk-devel

    install_centos_any_maven
}

install_ubuntu_20.04() {
    apt-get -y install \
        wget curl tar zip unzip git \
        build-essential ccache cmake ninja-build pkg-config autoconf libtool \
        flex bison \
        openjdk-8-jdk maven
}

install_ubuntu_22.04() { install_ubuntu_20.04; }

install_alinux_3() {
    yum -y groupinstall "Development Tools"
    yum -y install \
        wget curl tar zip unzip git which \
        cmake ninja-build perl-IPC-Cmd autoconf automake libtool \
        libstdc++-static flex bison python3 \
        java-1.8.0-openjdk java-1.8.0-openjdk-devel
}

## Install function end

## Usage
if [ -n "$*" ]; then
    cat <<EOF
Usage: sudo $0
Install build dependencies on the system.
EOF
    exit 1
fi

## Check and install depends

log_fatal() {
    echo "$@" >&2
    exit 1
}

[ "$(id -u)" == 0 ] || log_fatal "Root is required"

[ -f /etc/os-release ] || log_fatal "Failed to detect os: /etc/os-release in not exists"
eval "$(sed -En "/^(VERSION_|)ID=/s/^/OS_/p" /etc/os-release)"

[ -n "$OS_ID" -a -n "$OS_VERSION_ID" ] || log_fatal "Failed to detect os: ID or VERSION_ID is empty"

INSTALL_FUNC="install_${OS_ID}_${OS_VERSION_ID}"
[ "$(type -t "$INSTALL_FUNC")" == function ] || log_fatal "Unsupport OS: ${OS_ID} ${OS_VERSION_ID}"

set -x
$INSTALL_FUNC

echo "Success" >&2