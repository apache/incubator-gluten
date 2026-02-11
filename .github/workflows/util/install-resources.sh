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

# Download Spark resources, required by some Spark UTs. The resource path should be set
# for spark.test.home in mvn test.
#
# This file can be:
# 1. Executed directly: ./install-resources.sh <spark-version> [install-dir]
# 2. Sourced to use functions: source install-resources.sh; install_hadoop; setup_hdfs

set -e

# Install Hadoop binary
function install_hadoop() {
  echo "Installing Hadoop..."
  
  apt-get update -y
  apt-get install -y curl tar gzip
  
  local HADOOP_VERSION=3.3.6
  curl -fsSL -o hadoop.tgz "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz"
  tar -xzf hadoop.tgz --no-same-owner --no-same-permissions
  rm -f hadoop.tgz

  export HADOOP_HOME="$PWD/hadoop-${HADOOP_VERSION}"
  export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"
  export LD_LIBRARY_PATH="$HADOOP_HOME/lib/native:$LD_LIBRARY_PATH"

  if [ -n "$GITHUB_ENV" ]; then
    echo "HADOOP_HOME=$HADOOP_HOME" >> $GITHUB_ENV
    echo "LD_LIBRARY_PATH=$HADOOP_HOME/lib/native:$LD_LIBRARY_PATH" >> $GITHUB_ENV
    echo "$HADOOP_HOME/bin" >> $GITHUB_PATH
  fi
}

# Setup HDFS namenode and datanode
function setup_hdfs() {
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
  <property><name>dfs.replication</name><value>1</value></property>
  <property><name>dfs.namenode.rpc-address</name><value>localhost:9000</value></property>
  <property><name>dfs.namenode.http-address</name><value>localhost:9870</value></property>
  <property><name>dfs.datanode.address</name><value>localhost:9866</value></property>
  <property><name>dfs.datanode.http.address</name><value>localhost:9864</value></property>
  <property><name>dfs.permissions.enabled</name><value>false</value></property>
</configuration>
EOF

  HDFS_TMP="${RUNNER_TEMP:-/tmp}/hdfs"
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

# Installs Spark binary and source releases with:
# 1 - spark version
# 2 - hadoop version
# 3 - scala version
function install_spark() {
  local spark_version="$1"
  local hadoop_version="$2"
  local scala_version="$3"
  local spark_version_short=$(echo "${spark_version}" | cut -d '.' -f 1,2 | tr -d '.')
  local scala_suffix=$([ "${scala_version}" == '2.13' ] && echo '-scala-2.13' || echo '')
  local scala_suffix_short=$([ "${scala_version}" == '2.13' ] && echo '-scala2.13' || echo '')
  local mirror_host='https://www.apache.org/dyn/closer.lua/'
  local mirror_host2='https://mirror.lyrahosting.com/apache/' # Fallback mirror due to closer.lua slowness
  local url_query='?action=download'
  local checksum_suffix='sha512'
  local url_path="spark/spark-${spark_version}/"
  local local_binary="spark-${spark_version}-bin-hadoop${hadoop_version}${scala_suffix_short}.tgz"
  local local_binary_checksum="${local_binary}.${checksum_suffix}"
  local local_source="spark-${spark_version}.tgz"
  local local_source_checksum="${local_source}.${checksum_suffix}"
  local remote_binary="${mirror_host2}${url_path}${local_binary}${url_query}"
  local remote_binary_checksum="${mirror_host}${url_path}${local_binary_checksum}${url_query}"
  local remote_source="${mirror_host2}${url_path}${local_source}${url_query}"
  local remote_source_checksum="${mirror_host}${url_path}${local_source_checksum}${url_query}"
  local wget_opts="--no-verbose --no-check-certificate"

  wget ${wget_opts} -O "${local_binary}" "${remote_binary}"
  wget ${wget_opts} -O "${local_source}" "${remote_source}"

  # Checksum may not have been specified; don't check if doesn't exist
  if [ "$(command -v shasum)" ]; then
    wget ${wget_opts} -O "${local_binary_checksum}" "${remote_binary_checksum}"
    if ! shasum -a 512 -c "${local_binary_checksum}" > /dev/null ; then
      echo "Bad checksum from ${remote_binary_checksum}"
      rm -f "${local_binary_checksum}"
      exit 2
    fi
    rm -f "${local_binary_checksum}"

    wget ${wget_opts} -O "${local_source_checksum}" "${remote_source_checksum}"
    if ! shasum -a 512 -c "${local_source_checksum}" > /dev/null ; then
      echo "Bad checksum from ${remote_source_checksum}"
      rm -f "${local_source_checksum}"
      exit 2
    fi
    rm -f "${local_source_checksum}"
  else
    echo "Skipping checksum because shasum is not installed." 1>&2
  fi

  tar --strip-components=1 -xf "${local_binary}" spark-"${spark_version}"-bin-hadoop"${hadoop_version}""${scala_suffix_short}"/jars/ \
    spark-"${spark_version}"-bin-hadoop"${hadoop_version}""${scala_suffix_short}"/python \
    spark-"${spark_version}"-bin-hadoop"${hadoop_version}""${scala_suffix_short}"/bin
  mkdir -p ${INSTALL_DIR}/shims/spark"${spark_version_short}""${scala_suffix}"/spark_home/assembly/target/scala-"${scala_version}"
  mv jars ${INSTALL_DIR}/shims/spark"${spark_version_short}""${scala_suffix}"/spark_home/assembly/target/scala-"${scala_version}"
  mv python ${INSTALL_DIR}/shims/spark"${spark_version_short}""${scala_suffix}"/spark_home
  mv bin ${INSTALL_DIR}/shims/spark"${spark_version_short}""${scala_suffix}"/spark_home

  tar --strip-components=1 -xf "${local_source}" spark-"${spark_version}"/sql/core/src/test/resources/
  mkdir -p shims/spark"${spark_version_short}${scala_suffix}"/spark_home/
  mv sql shims/spark"${spark_version_short}${scala_suffix}"/spark_home/

  rm -rf "${local_binary}"
  rm -rf "${local_source}"
}

# Only run install_spark when script is executed directly (not sourced)
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  INSTALL_DIR=${2:-/opt/}
  mkdir -p ${INSTALL_DIR}

  case "$1" in
  3.3)
      # Spark-3.3
      cd ${INSTALL_DIR} && \
      install_spark "3.3.1" "3" "2.12"
      ;;
  3.4)
      # Spark-3.4
      cd ${INSTALL_DIR} && \
      install_spark "3.4.4" "3" "2.12"
      ;;
  3.5)
      # Spark-3.5
      cd ${INSTALL_DIR} && \
      install_spark "3.5.5" "3" "2.12"
      ;;
  3.5-scala2.13)
      # Spark-3.5, scala 2.13
      cd ${INSTALL_DIR} && \
      install_spark "3.5.5" "3" "2.13"
      ;;
  4.0)
      # Spark-4.0, scala 2.12 // using 2.12 as a hack as 4.0 does not have 2.13 suffix
      cd ${INSTALL_DIR} && \
      install_spark "4.0.1" "3" "2.12"
      ;;
  4.1)
      # Spark-4.x, scala 2.12 // using 2.12 as a hack as 4.0 does not have 2.13 suffix
      cd ${INSTALL_DIR} && \
      install_spark "4.1.1" "3" "2.12"
      ;;
  *)
      echo "Spark version is expected to be specified."
      exit 1
      ;;
  esac
fi
