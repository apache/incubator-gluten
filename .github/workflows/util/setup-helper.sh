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
  export PATH=/usr/lib/maven/bin:$PATH
  if [ -n "$GITHUB_ENV" ]; then
    echo "PATH=/usr/lib/maven/bin:$PATH" >> $GITHUB_ENV
  else
    echo "Warning: GITHUB_ENV is not set. Skipping environment variable export."
  fi
}

function install_hadoop {
  echo "Installing Hadoop..."
  
  apt-get update -y
  apt-get install -y curl tar gzip
  
  local HADOOP_VERSION=3.3.6
  curl -fsSL -o hadoop.tgz "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz"
  tar -xzf hadoop.tgz --no-same-owner --no-same-permissions

  export HADOOP_HOME="$PWD/hadoop-${HADOOP_VERSION}"
  export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"

  export LD_LIBRARY_PATH="$HADOOP_HOME/lib/native:$LD_LIBRARY_PATH"

  if [ -n "$GITHUB_ENV" ]; then
    echo "HADOOP_HOME=$HADOOP_HOME" >> $GITHUB_ENV
    echo "LD_LIBRARY_PATH=$HADOOP_HOME/lib/native:$LD_LIBRARY_PATH" >> $GITHUB_ENV
    echo "$HADOOP_HOME/bin" >> $GITHUB_PATH
  fi
}

function setup_hdfs {
  export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"

  cat > "$HADOOP_CONF_DIR/core-site.xml" <<'EOF'
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
</configuration>
EOF

  cat > "$HADOOP_CONF_DIR/hdfs-site.xml" <<'EOF'
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>

  <property>
    <name>dfs.namenode.rpc-address</name>
    <value>localhost:9000</value>
  </property>

  <property>
    <name>dfs.namenode.http-address</name>
    <value>localhost:9870</value>
  </property>

  <property>
    <name>dfs.datanode.address</name>
    <value>localhost:9866</value>
  </property>

  <property>
    <name>dfs.datanode.http.address</name>
    <value>localhost:9864</value>
  </property>

  <property>
    <name>dfs.permissions.enabled</name>
    <value>false</value>
  </property>
</configuration>
EOF

  export HDFS_TMP="${RUNNER_TEMP:-/tmp}/hdfs"
  mkdir -p "$HDFS_TMP/nn" "$HDFS_TMP/dn"

  perl -0777 -i -pe 's#</configuration>#  <property>\n    <name>dfs.namenode.name.dir</name>\n    <value>file:'"$HDFS_TMP"'/nn</value>\n  </property>\n  <property>\n    <name>dfs.datanode.data.dir</name>\n    <value>file:'"$HDFS_TMP"'/dn</value>\n  </property>\n</configuration>#s' \
    "$HADOOP_CONF_DIR/hdfs-site.xml"

  if [ -n "${GITHUB_ENV:-}" ]; then
    echo "HADOOP_CONF_DIR=$HADOOP_CONF_DIR" >> "$GITHUB_ENV"
    echo "HADOOP_HOME=$HADOOP_HOME" >> "$GITHUB_ENV"
  fi

  "$HADOOP_HOME/bin/hdfs" namenode -format -force -nonInteractive
  "$HADOOP_HOME/sbin/hadoop-daemon.sh" start namenode
  "$HADOOP_HOME/sbin/hadoop-daemon.sh" start datanode

  for i in {1..60}; do
    "$HADOOP_HOME/bin/hdfs" dfs -ls / >/dev/null 2>&1 && break
    sleep 1
  done

  "$HADOOP_HOME/bin/hdfs" dfs -ls /
}
for cmd in "$@"
do
    echo "Running: $cmd"
    "$cmd"
done
