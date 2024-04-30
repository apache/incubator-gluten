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

# install dependencies
wget -nv http://archive.apache.org/dist/thrift/0.12.0/thrift-0.12.0.tar.gz
tar xzf thrift-0.12.0.tar.gz
cd thrift-0.12.0
chmod +x ./configure
./configure --disable-libs
sudo make install
# build parquet-mr, will need private branch after refine DataGenerator
git clone https://github.com/apache/parquet-mr
cd parquet-mr
git checkout parquet-1.11.x
LC_ALL=C mvn clean package -DskipTests -Denforcer.skip=true -pl parquet-benchmarks -am
# generate parquet files
java -cp parquet-benchmarks/target/parquet-benchmarks.jar org.apache.parquet.benchmarks.DataGenerator generate
