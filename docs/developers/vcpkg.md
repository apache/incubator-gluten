---
layout: page
title: Setup depends via vcpkg
nav_order: 9
parent: Developer Overview
---

# Setup static build environment via vcpkg

## Setup build depends

Please install build depends on your system to compile all libraries:

``` sh
sudo $GLUTEN_REPO/dev/vcpkg/setup-build-depends.sh
```

For CentOS user, gcc 9 needs to be enabled manually before next step:

``` sh
# CentOS 8
source /opt/rh/gcc-toolset-9/enable

# CentOS 7
source /opt/rh/devtoolset-9/enable
```

For unsupported linux distro, you can install the following packages from package manager.

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
* openjdk 8
* maven

## Install depends and setup build environment

Simply run:

``` sh
source $GLUTEN_REPO/dev/vcpkg/env.sh
```

This script will install all static libraries into the `$GLUTEN_REPO/dev/vcpkg/vcpkg_installed/`
directory and set the `$PATH` and `$CMAKE_TOOLCHAIN_FILE`.
This make build systems to locate the binary tools and libraries.
It will take about 15~30 minutes to download and build all dependencies from source.
You can configure [binary cache](https://learn.microsoft.com/en-us/vcpkg/users/binarycaching) to accelerate the next setup.