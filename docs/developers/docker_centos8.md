---
layout: page
title: Docker script for CentOS 8
nav_order: 7
parent: Developer Overview
---
Here is a docker script we verified to build Gluten+Velox backend on Centos8:

Run on host as root user:
```
docker pull centos:8
docker run -itd --name gluten centos:8 /bin/bash
docker attach gluten
```

Run in docker:
```
#update mirror
sed -i -e "s|mirrorlist=|#mirrorlist=|g" /etc/yum.repos.d/CentOS-*
sed -i -e "s|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g" /etc/yum.repos.d/CentOS-*

dnf install -y epel-release sudo
yum install -y dnf-plugins-core
yum config-manager --set-enabled powertools
dnf --enablerepo=powertools install -y ninja-build
dnf --enablerepo=powertools install -y libdwarf-devel
dnf install -y --setopt=install_weak_deps=False ccache gcc-toolset-9 git wget which libevent-devel \
  openssl-devel re2-devel libzstd-devel lz4-devel double-conversion-devel \
  curl-devel cmake libicu-devel

source /opt/rh/gcc-toolset-9/enable || exit 1

yum install -y java-1.8.0-openjdk-devel patch
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk
export PATH=$JAVA_HOME/bin:$PATH

#gluten need maven version >=3.6.3
wget https://downloads.apache.org/maven/maven-3/3.8.8/binaries/apache-maven-3.8.8-bin.tar.gz
tar -xvf apache-maven-3.8.8-bin.tar.gz
mv apache-maven-3.8.8 /usr/lib/maven
export MAVEN_HOME=/usr/lib/maven
export PATH=${PATH}:${MAVEN_HOME}/bin

git clone https://github.com/oap-project/gluten.git
cd gluten

# To access HDFS or S3, you need to add the parameters `--enable_hdfs=ON` and `--enable_s3=ON`
./dev/buildbundle-veloxbe.sh
```