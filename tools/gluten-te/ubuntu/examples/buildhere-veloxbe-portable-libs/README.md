# Utility for building C++ libs in container

The folder contains script code to build `libvelox.so` and `libgluten.so` in docker container.

The built shared library files should be portable among different X86 + Linux environments, and can be used from host for building Gluten's fat Jar using Maven.

## Prerequisites

1. X86 CPU architecture
2. Host machine with Linux operating system
3. Docker

## Usage

```sh
# 1. Set the following envs in case you are behind http proxy.
export HTTP_PROXY_HOST=myproxy.example.com
export HTTP_PROXY_PORT=55555

# 2. Build the C++ libs in a ubuntu 20.04 docker container.
cd gluten/
tools/gluten-te/ubuntu/examples/buildhere-veloxbe-portable-libs/run.sh

# 3. Check the built libs.
ls -l cpp/build/releases/
```