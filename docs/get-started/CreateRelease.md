---
layout: page
title: Create a Release Distribution
nav_order: 5
parent: Getting-Started
---

The document introduces a standard way for creating a release distribution of Apache Gluten
project with the Velox backend.

## Prerequisites

1. x86-64
2. Linux
3. Docker

## Steps to Create a Release

A standard release distribution can be created following the below steps.

### Pull and run the dev docker image

The docker image to be pulled for creating the release is an image that is periodically
built and upload to DockerHub.

```bash
docker pull apache/gluten:vcpkg-centos-7
docker run -it apache/gluten:vcpkg-centos-7 bash
```

### Clone the repository

In the docker container created by the last step, execute the following command to
clone the repository of Gluten with a specific git tag that you want to build on.

We are taking `v1.6.0-example-rc3` as an example git tag in this guide.

```bash
git clone --branch v1.6.0-example-rc3 https://github.com/apache/incubator-gluten.git /workspace
```

### Build

Build the project for all supported Spark versions.

```bash
cd /workspace
bash dev/release/build-release.sh
```

### Package the release sources and binaries

This step creates the release distribution that comply with the common name convention
of ASF project release.

Note, the current build name containing the build tag and a release candidate ID should be
specified when running this script.

```bash
cd /workspace
bash dev/release/package-release.sh v1.6.0-example-rc3
```

### Check the created release distribution

Confirm that all the needed sources and binaries are successfully created.

```bash
[root@8de83f716f0f workspace]# ls -l release/
total 481628
-rw-r--r--. 1 root root  74396439 Oct 14 14:19 apache-gluten-1.6.0-example-incubating-src.tar.gz
-rw-r--r--. 1 root root 104790092 Oct 14 14:19 apache-gluten-1.6.0-example-incubating-bin-spark-3.2.tar.gz
-rw-r--r--. 1 root root 104767582 Oct 14 14:19 apache-gluten-1.6.0-example-incubating-bin-spark-3.3.tar.gz
-rw-r--r--. 1 root root 104625356 Oct 14 14:19 apache-gluten-1.6.0-example-incubating-bin-spark-3.4.tar.gz
-rw-r--r--. 1 root root 104595103 Oct 14 14:19 apache-gluten-1.6.0-example-incubating-bin-spark-3.5.tar.gz
```
