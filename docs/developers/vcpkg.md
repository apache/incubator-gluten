---
layout: page
title: Setup depends via vcpkg
nav_order: 9
parent: Developer Overview
---

# Setup static native depends via vcpkg

## Setup build depends

Please install build depends to compile all libraries:

``` sh
sudo $GLUTEN_REPO/dev/vcpkg/setup-build-depends.sh
```

You can install the following tools manually on unsupport distro.

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
source $GLUTEN_REPO/dev/vcpkg/env.sh
```

This script will install all static libraries into the `vcpkg_installed/`
directory and set the `$PATH` and `$CMAKE_TOOLCHAIN_FILE`.
This allows cmake to locate the appropriate header files and libraries.
Under good network conditions, it will take approximately 25 minutes.
You can configure [binary cache](https://learn.microsoft.com/en-us/vcpkg/users/binarycaching) to accelerate the next setup.