FROM ghcr.io/facebookincubator/velox-dev:adapters
RUN yum install -y sudo patch maven perl
# After the base docker image updated, don't need to install cmake
RUN pip install cmake==3.30.4 && ln -s /usr/local/bin/cmake /usr/bin

RUN git clone -b cudf --depth=1 https://github.com/apache/incubator-gluten /opt/gluten

# Install spark to folder /opt
RUN cd /opt/gluten/.github/workflows/util/ && ./install_spark_resources.sh 3.4
ENV SPARK_HOME=/opt/spark-3.4.4-bin-hadoop3
ENV PATH=$SPARK_HOME/bin:$PATH
ENV CUDA_ARCHITECTURES=70

WORKDIR /opt/gluten
RUN ./dev/buildbundle-veloxbe.sh --run_setup_script=ON --build_arrow=ON --spark_version=3.4 --enable_gpu=ON && \
    touch cudf.sql && echo "select o_orderkey from orders order by o_orderkey;" > cudf.sql

# You can try the data in folder backends-velox/src/test/resources/tpch-data-parquet
