# Utility for building C++ libs in container

The folder contains script code to build `libvelox.so` and `libgluten.so` in docker container and for host use.

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

# 4. If you intend to build Gluten's bundled jar, continue running subsequent Maven commands.
# For example:
mvn clean install -P spark-3.4,backends-velox -DskipTests
```