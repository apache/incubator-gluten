# Utility for building C++ libs in CentOS 7 (with glibc 2.17) container

The folder contains script code to build `libvelox.so` and `libgluten.so` in docker container and for host use.

## Prerequisites

1. X86 CPU architecture
2. Host machine with Linux operating system
3. Docker

## Usage

```sh
# 1. (Optional) Set the following envs in case you are behind http proxy.
export HTTP_PROXY_HOST=myproxy.example.com
export HTTP_PROXY_PORT=55555

# 2. Build the C++ libs in a centos 7 docker container.
# Note, this command could take much longer time to finish if it's never run before.
# After the first run, the essential build environment will be cached in docker builder.
#
# Additionally, changes to HTTP_PROXY_HOST / HTTP_PROXY_PORT could invalidate the build cache
# either. For more details, please check docker file `dockerfile-buildenv`.
cd gluten/
tools/gluten-te/centos/examples/buildhere-veloxbe-portable-libs/run-default.sh

# 3. Check the built libs.
ls -l cpp/build/releases/

# 4. If you intend to build Gluten's bundled jar, continue running subsequent Maven commands.
# For example:
mvn clean install -P spark-3.4,backends-velox -DskipTests
```