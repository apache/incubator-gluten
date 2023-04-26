# Install native depends via vcpkg

## Setup build depends

Please install the following packages to compile all libraries.

### Ubuntu 20.04 & 22.04

``` sh
apt-get install \
  wget curl tar zip unzip git \
  build-essential ccache cmake ninja-build pkg-config autoconf libtool \
  flex bison \
  openjdk-8-jdk maven
```

### CentOS 8

``` sh
yum install \
    wget curl tar zip unzip git which \
    cmake ninja-build perl-IPC-Cmd autoconf automake libtool \
    gcc-toolset-9-gcc gcc-toolset-9-gcc-c++ \
    flex bison python3 \
    java-1.8.0-openjdk java-1.8.0-openjdk-devel
```

### CentOS 7

``` sh
yum install epel-release centos-release-scl
yum -y install \
    wget curl tar zip unzip which \
    cmake3 ninja-build perl-IPC-Cmd autoconf automake libtool \
    devtoolset-9 \
    bison \
    java-1.8.0-openjdk java-1.8.0-openjdk-devel \
```

`git >= 2.7.4` and `flex >= 2.6.0` is required. You will need to install them from other source on CentOS 7.

``` sh
# Install latest git from endpointdev repo
yum install https://packages.endpointdev.com/rhel/7/os/x86_64/endpoint-repo.x86_64.rpm
yum install git
```

``` sh
# Install flex 2.6.4 from source
FLEX_URL="https://github.com/westes/flex/releases/download/v2.6.4/flex-2.6.4.tar.gz"
mkdir -p /tmp/flex
wget -q --max-redirect 3 -O - "${FLEX_URL}" | tar -xz -C /tmp/flex --strip-components=1
cd /tmp/flex
./autogen.sh
./configure
make install
cd
rm -rf /tmp/flex
```

### Other

Other distributions are not officially supported now.
But you can try installing the following tools.

* zip
* tar
* wget
* curl
* git >= 2.7.4
* gcc >= 9
* pkg-config
* autotools
* flex >= 2.6.0
* bison

## Install depends and setup build environment

Simply run:

``` sh
source ./env.sh
```

This script will install all static libraries into the `vcpkg_installed/`
directory and set the `$PATH` and `$CMAKE_TOOLCHAIN_FILE`.
This allows cmake to locate the appropriate header files and libraries.
Under good network conditions, it will take approximately 25 minutes.
You can configure [binary cache](https://learn.microsoft.com/en-us/vcpkg/users/binarycaching) to accelerate the next setup.