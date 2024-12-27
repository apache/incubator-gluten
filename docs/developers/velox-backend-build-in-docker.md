---
layout: page
title: Build Gluten Velox backend in docker
nav_order: 7
parent: Developer Overview
---

Currently, Centos-7/8/9 and Ubuntu 20.04/22.04 are supported to build Gluten Velox backend. Please refer to
`.github/workflows/velox_weekly.yml` to install required tools before the build.

There are two docker images with almost all dependencies installed, respective for static build and dynamic build.
The according Dockerfiles are respectively `Dockerfile.centos7-static-build` and `Dockerfile.centos8-dynamic-build`
under `dev/docker/`.

```shell
# For static build on centos-7.
docker pull apache/gluten:vcpkg-centos-7

# For dynamic build on centos-8.
docker pull apache/gluten:centos-8 (dynamic build)
```
