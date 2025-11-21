#
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
#

FROM ghcr.io/facebookincubator/velox-dev:adapters
RUN yum install -y sudo patch maven perl && ln -sf /usr/local/bin/cmake /usr/bin

RUN git clone --depth=1 https://github.com/apache/incubator-gluten /opt/gluten

# Install spark to folder /opt
RUN cd /opt/gluten/.github/workflows/util/ && ./install-spark-resources.sh 3.4
ENV SPARK_HOME=/opt/spark-3.4.4-bin-hadoop3
ENV PATH=$SPARK_HOME/bin:$PATH
ENV CUDA_ARCHITECTURES=70


WORKDIR /opt/gluten
RUN bash ./dev/buildbundle-veloxbe.sh --run_setup_script=OFF --build_arrow=ON --spark_version=3.4 --build_tests=ON --build_benchmarks=ON --enable_gpu=ON && rm -rf /opt/gluten

# You can try the data in folder backends-velox/src/test/resources/tpch-data-parquet

