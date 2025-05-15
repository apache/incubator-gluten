---
layout: page
title: Velox Backend CI
nav_order: 6
parent: Developer Overview
---
# Velox Backend CI

GHA workflows are defined under `.github/workflows/`.

## Docker Build
We have a weekly job defined in `docker_image.yml` to build docker images for CI verification. The docker files and images are listed below:

file | images | comments
-- | -- | --
dev/docker/Dockerfile.centos7-static-build | apache/gluten:vcpkg-centos-7 | centos 7, static link, jdk8
dev/docker/Dockerfile.centos8-static-build | apache/gluten:vcpkg-centos-8 | centos 8, static link, jdk8
dev/docker/Dockerfile.centos8-dynamic-build | apache/gluten:centos-8-jdk8 | centos 8, dynamic link, jdk8
dev/docker/Dockerfile.centos8-dynamic-build | apache/gluten:centos-8-jdk11 | centos 8, dynamic link, jdk11
dev/docker/Dockerfile.centos8-dynamic-build | apache/gluten:centos-8-jdk17 | centos 8, dynamic link, jdk17
dev/docker/cudf/Dockerfile | apache/gluten:centos-9-jdk8-cudf | centos 9, dynamic link, jdk8

Docker images can be found from https://hub.docker.com/r/apache/gluten/tags

## Vcpkg Caching
Gluten main branch is pulled down during static build in docker. And vcpkg will cache binary data of all dependencies defined under dev/vcpkg.
These binary data is cached into `/var/cache/vcpkg` and CI job can re-use them in new build. By setting `VCPKG_BINARY_SOURCES=clear` in env.,
reusing vcpkg cache can be disabled.

## Arrow Libs Pre-installation
Arrow libs are pre-installed in docker, assuming they are not actively changed, then not necessarily to be re-built every time.

## .M2 cache
The dependency libraries are pre installed in to /root/.m2 by `mvn dependency:go-offline` Spark is set to 3.5 by default.

## Ccache
Since the docker image is rebuilt weekly, the ccache mostly are outdated. So the cache is removed from the image.

## Updating Docker Image
Two GitHub secrets `DOCKERHUB_USER` & `DOCKERHUB_TOKEN` can be used to push docker image to docker hub: https://hub.docker.com/r/apache/gluten/tags.
Note GitHub secrets are not retrievable in PR from forked repo.