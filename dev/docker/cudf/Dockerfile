FROM ghcr.io/facebookincubator/velox-dev:adapters
RUN yum install -y sudo patch maven perl && ln -sf /usr/local/bin/cmake /usr/bin

RUN git clone --depth=1 https://github.com/apache/incubator-gluten /opt/gluten

# Install spark to folder /opt
RUN cd /opt/gluten/.github/workflows/util/ && ./install-spark-resources.sh 3.4
ENV SPARK_HOME=/opt/spark-3.4.4-bin-hadoop3
ENV PATH=$SPARK_HOME/bin:$PATH
ENV CUDA_ARCHITECTURES=70

WORKDIR /opt/gluten
RUN bash ./dev/buildbundle-veloxbe.sh --run_setup_script=OFF --build_arrow=ON --spark_version=3.4 --enable_gpu=ON && rm -rf /opt/gluten

# You can try the data in folder backends-velox/src/test/resources/tpch-data-parquet

