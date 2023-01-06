FROM ubuntu:22.04
USER root
RUN apt-get update
RUN apt install -y software-properties-common
RUN apt install -y maven build-essential cmake libssl-dev libre2-dev libcurl4-openssl-dev clang lldb lld libz-dev git ninja-build uuid-dev wget sudo openjdk-8-jdk default-jdk vim
RUN apt install -y ccache ninja-build checkinstall libssl-dev libboost-all-dev libdouble-conversion-dev libgoogle-glog-dev libbz2-dev libgflags-dev libgmock-dev libevent-dev liblz4-dev libzstd-dev libre2-dev libsnappy-dev  liblzo2-dev bison flex wget
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

RUN wget https://archive.apache.org/dist/spark/spark-3.2.2/spark-3.2.2-bin-hadoop3.2.tgz
RUN tar -zxvf spark-3.2.2-bin-hadoop3.2.tgz
ENV PATH=/spark-3.2.2-bin-hadoop3.2/bin:$PATH


RUN ln -fs /usr/share/zoneinfo/America/Los_Angeles /etc/localtime
RUN DEBIAN_FRONTEND=noninteractive apt-get install -y tzdata
RUN dpkg --configure -a