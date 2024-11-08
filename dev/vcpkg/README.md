# Build Gluten + Velox in Vcpkg Environment

## Build in Docker

Please install make and docker on your system, then `make`.
The gluten packages will be placed in `$GLUTEN_REPO/package/target/gluten-velox-bundle-*.jar`.

## Setup build environment manually

### Setup build toolkits

Please install build depends on your system to compile all libraries:

``` sh
sudo $GLUTEN_REPO/dev/vcpkg/setup-build-depends.sh
```

GCC-11 is the minimum required compiler. It needs to be enabled beforehand. Take Centos-7/8 as example:

``` sh
# CentOS 8
source /opt/rh/gcc-toolset-11/enable

# CentOS 7
source /opt/rh/devtoolset-11/enable
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

### Build gluten + velox with vcpkg installed dependencies

With `--enable_vcpkg=ON`, the below script will install all static libraries into `./vcpkg_installed/`. And it will
also set `$PATH` and `$CMAKE_TOOLCHAIN_FILE` to make CMake to locate the binary tools and libraries.
You can configure [binary cache](https://learn.microsoft.com/en-us/vcpkg/users/binarycaching) to accelerate the build.

``` sh
$GLUTEN_REPO/dev/buildbundle-veloxbe.sh --enable_vcpkg=ON --build_tests=ON --build_benchmarks=ON --enable_s3=ON  --enable_hdfs=ON
```