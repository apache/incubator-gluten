#! /bin/sh
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -ex

sed -i -e "s|mirrorlist=|#mirrorlist=|g" /etc/yum.repos.d/CentOS-* 
# The connection to vault.centos.org in CI is unstable
# sed -i -e "s|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g" /etc/yum.repos.d/CentOS-*
minorver=8.5.2111
sed -i -e \
  "s|^#baseurl=http://mirror.centos.org/\$contentdir/\$releasever|baseurl=https://mirrors.aliyun.com/centos-vault/$minorver|g" \
  /etc/yum.repos.d/CentOS-*.repo

dnf install -y epel-release sudo
yum -y update && yum clean all && yum install -y dnf-plugins-core
yum config-manager --set-enabled powertools
dnf --enablerepo=powertools install -y ninja-build
dnf --enablerepo=powertools install -y libdwarf-devel
dnf install -y --setopt=install_weak_deps=False ccache gcc-toolset-9 git wget which libevent-devel \
  openssl-devel re2-devel libzstd-devel lz4-devel double-conversion-devel \
  curl-devel cmake libicu-devel

yum -y update && yum clean all && yum install -y java-1.8.0-openjdk-devel patch
dnf -y install gcc-toolset-9-gcc gcc-toolset-9-gcc-c++
