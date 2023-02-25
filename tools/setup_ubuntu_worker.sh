#!/bin/bash

# Minimal setup for Ubuntu 20.04

# Install all dependencies for jar release

# Install and Setup JDK8
sudo apt install -y openjdk-8-jdk
sudo apt install -y default-jdk
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

# Install Pre-requisite Libraries
sudo apt install -y libevent-dev=2.1
sudo apt install -y openssl-dev=1.1.1
sudo apt install -y libre2-dev=20200101+dfsg-1build1
sudo apt install -y libboost-all-dev=1.71.0.0ubuntu2 
sudo apt install -y libgflags-dev=2.2.2-1build1
sudo apt install -y libgoogle-glog-dev=0.4.0-1build1
sudo apt install -y libzstd-dev=1.4.8+dfsg-3build1
sudo apt install -y liblz-dev=1.9.3-2build2
sudo apt install -y libsnappy-dev=1.1.8-1build1
sudo apt install -y libdouble-conversion-dev=3.1.7-4
sudo apt install -y libiberty-dev=20200409-1
