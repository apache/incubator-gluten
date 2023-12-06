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

#CENTOS_MIRROR_URL=https://mirrors.edge.kernel.org/centos
#CENTOS_MIRROR_GPGKEY="${CENTOS_MIRROR_URL}/RPM-GPG-KEY-CentOS-7"
#
#cp /etc/yum.repos.d/CentOS-Base.repo /tmp/CentOS-Base.repo
#sed -i "/^mirrorlist/d;s/^\#baseurl=/baseurl=/" /tmp/CentOS-Base.repo
#sed -i "s|^gpgkey=.*$|gpgkey=${CENTOS_MIRROR_GPGKEY}|" /tmp/CentOS-Base.repo
#sed -i "s|http://mirror.centos.org/centos|${CENTOS_MIRROR_URL}|" /tmp/CentOS-Base.repo
#rm /etc/yum.repos.d/*
#mv /tmp/CentOS-Base.repo /etc/yum.repos.d/

sed -e 's|^mirrorlist=|#mirrorlist=|g' \
         -e 's|^#baseurl=http://mirror.centos.org/centos|baseurl=https://mirrors.ustc.edu.cn/centos|g' \
         -i.bak \
         /etc/yum.repos.d/CentOS-Base.repo

# Disable fastestmirror
sed -i "s/enabled=1/enabled=0/" /etc/yum/pluginconf.d/fastestmirror.conf 

yum -y install epel-release centos-release-scl
yum -y install \
    git \
    dnf \
    cmake3 \
    ccache \
    devtoolset-9 \
    java-1.8.0-openjdk \
    java-1.8.0-openjdk-devel \
    ninja-build \
    wget

ln -s /usr/bin/cmake3 /usr/local/bin/cmake
