---
layout: page
title: Docker script for Ubuntu 22.04/20.04
nav_order: 6
parent: Developer Overview
---

To the first build, it's suggested to build Gluten in a clean docker image. Otherwise it's easy to run into library version conflict issues.
Here is a docker script we verified to build Gluten+Velox backend on Ubuntu22.04/20.04:

Run on host as root user:
```
docker pull ubuntu:22.04
docker run -itd --network host --name gluten ubuntu:22.04 /bin/bash
docker attach gluten
```

Run in docker:
```
apt-get update

#install gcc and libraries to build arrow
apt install software-properties-common
apt install maven build-essential cmake libssl-dev libre2-dev libcurl4-openssl-dev clang lldb lld libz-dev git ninja-build uuid-dev autoconf-archive curl zip unzip tar pkg-config bison libtool flex vim

#velox script needs sudo to install dependency libraries
apt install sudo

# make sure jemalloc is uninstalled, jemalloc will be build in vcpkg, which conflicts with the default jemalloc in system
apt purge libjemalloc-dev libjemalloc2 librust-jemalloc-sys-dev

#make sure jdk8 is used. New version of jdk is not supported
apt install -y openjdk-8-jdk
apt install -y default-jdk
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

#manually install tzdata to avoid the interactive timezone config
ln -fs /usr/share/zoneinfo/America/New_York /etc/localtime
DEBIAN_FRONTEND=noninteractive apt-get install -y tzdata
dpkg --configure -a

#setup proxy on necessary
#export http_proxy=xxxx
#export https_proxy=xxxx

#clone gluten
git clone https://github.com/apache/incubator-gluten.git
cd incubator-gluten/

#config maven proxy
#mkdir ~/.m2/
#vim ~/.m2/settings.xml

# the script download velox & arrow and compile all dependency library automatically
# To access HDFS or S3, you need to add the parameters `--enable_hdfs=ON` and `--enable_s3=ON`
# It's suggested to build using static link, enabled by `--enable_vcpkg=ON`
# For developer, it's suggested to enable Debug info, by --build_type=RelWithDebInfo. Note RelWithDebInfo uses -o2, release uses -o3
./dev/buildbundle-veloxbe.sh --enable_vcpkg=ON --build_type=RelWithDebInfo
```
