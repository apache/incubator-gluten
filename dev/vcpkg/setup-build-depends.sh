#! /bin/bash

set -e

## Install functions begin

function semver {
    echo "$@" | awk -F. '{ printf("%d%03d%03d", $1,$2,$3); }'
}

install_centos_any_maven() {
    if [ -z "$(which mvn)" ]; then
        maven_version=3.9.2
        maven_install_dir=/opt/maven-$maven_version
        if [ -d /opt/maven-$maven_version ]; then
            echo "Failed to install maven: ${maven_install_dir} is exists" >&2
            exit 1
        fi

        cd /tmp
        wget https://archive.apache.org/dist/maven/maven-3/$maven_version/binaries/apache-maven-$maven_version-bin.tar.gz
        tar -xvf apache-maven-$maven_version-bin.tar.gz
        rm apache-maven-$maven_version-bin.tar.gz
        mv apache-maven-$maven_version "${maven_install_dir}"
        ln -s "${maven_install_dir}/bin/mvn" /usr/local/bin/mvn
    fi
}

install_centos_7() {
    export PATH=/usr/local/bin:$PATH

    yum -y install epel-release centos-release-scl
    yum -y install \
        wget curl tar zip unzip which \
        cmake3 ninja-build perl-IPC-Cmd autoconf autoconf-archive automake libtool \
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

    # automake>=1.14
    installed_automake_version="$(aclocal --version | sed -En "1s/^.* ([1-9\.]*)$/\1/p")"
    if [ "$(semver "$installed_automake_version")" -lt "$(semver 1.14)" ]; then
        mkdir -p /tmp/automake
        wget -O - http://ftp.gnu.org/gnu/automake/automake-1.16.5.tar.xz | tar -x --xz -C /tmp/automake --strip-components=1
        cd /tmp/automake
        ./configure
        make install -j
        cd
        rm -rf /tmp/automake

        # Fix aclocal search path
        echo /usr/share/aclocal > /usr/local/share/aclocal/dirlist
    fi

    install_centos_any_maven
}

install_centos_8() {
    yum -y install \
        wget curl tar zip unzip git which \
        cmake ninja-build perl-IPC-Cmd autoconf autoconf-archive automake libtool \
        gcc-toolset-9-gcc gcc-toolset-9-gcc-c++ \
        flex bison python3 \
        java-1.8.0-openjdk java-1.8.0-openjdk-devel

    install_centos_any_maven
}

install_ubuntu_20.04() {
    apt-get -y install \
        wget curl tar zip unzip git \
        build-essential ccache cmake ninja-build pkg-config autoconf autoconf-archive libtool \
        flex bison \
        openjdk-8-jdk maven
}

install_ubuntu_22.04() { install_ubuntu_20.04; }

install_alinux_3() {
    yum -y groupinstall "Development Tools"
    yum -y install \
        wget curl tar zip unzip git which \
        cmake ninja-build perl-IPC-Cmd autoconf autoconf-archive automake libtool \
        libstdc++-static flex bison python3 \
        java-1.8.0-openjdk java-1.8.0-openjdk-devel
}

install_tencentos_3.2() {
    yum -y install \
        wget curl tar zip unzip git which \
        cmake ninja-build perl-IPC-Cmd autoconf autoconf-archive automake libtool \
        gcc-toolset-9-gcc gcc-toolset-9-gcc-c++ \
        flex bison python3 \
        java-8-konajdk

    install_centos_any_maven
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