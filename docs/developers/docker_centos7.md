---
layout: page
title: Docker script for CentOS 7
nav_order: 8
parent: Developer Overview
---
Here is a docker script we verified to build Gluten+Velox backend on CentOS 7:

Run on host as root user:
```
docker pull centos:7
docker run -itd --name gluten centos:7 /bin/bash
docker attach gluten
```

Run in docker:
```
yum -y install epel-release centos-release-scl
yum -y install \
    git \
    dnf \
    cmake3 \
    devtoolset-9 \
    java-1.8.0-openjdk \
    java-1.8.0-openjdk-devel \
    ninja-build \
    wget

# gluten need maven version >=3.6.3
wget https://downloads.apache.org/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.tar.gz
tar -xvf apache-maven-3.6.3-bin.tar.gz
mv apache-maven-3.6.3 /usr/lib/maven
export MAVEN_HOME=/usr/lib/maven
export PATH=${PATH}:${MAVEN_HOME}/bin

# cmake 3.x is required
ln -s /usr/bin/cmake3 /usr/local/bin/cmake

# enable gcc 9
. /opt/rh/devtoolset-9/enable || exit 1

export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk
export PATH=$JAVA_HOME/bin:$PATH

git clone https://github.com/oap-project/gluten.git
cd gluten
./dev/buildbundle-veloxbe.sh
```
