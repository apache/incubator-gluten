#!/bin/bash
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

set -e

function install_maven {
  (
    local maven_version="3.9.2"
    local local_binary="apache-maven-${maven_version}-bin.tar.gz"
    local mirror_host="https://www.apache.org/dyn/closer.lua"
    local url="${mirror_host}/maven/maven-3/${maven_version}/binaries/${local_binary}?action=download"
    cd /opt/
    wget -nv -O ${local_binary} ${url}
    tar -xvf ${local_binary} && mv apache-maven-${maven_version} /usr/lib/maven
  )
  echo "PATH=${PATH}:/usr/lib/maven/bin" >> $GITHUB_ENV
}

for cmd in "$@"
do
    echo "Running: $cmd"
    "$cmd"
done
