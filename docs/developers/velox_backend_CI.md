---
layout: page
title: CPP Code Style
nav_order: 14
parent: Developer Overview
---
# Velox Backend CI

GHA workflows are defined under `.github/workflows/`.

## Docker Build
We have a weekly job to build a docker based on Dockerfile.gha for CI verification, defined in docker_image.yml.

## Vcpkg Caching
Gluten main branch is pulled down during docker build. And vcpkg will cache binary data of all dependencies defined under dev/vcpkg.
These binary data is cached into `/var/cache/vcpkg` and CI job can re-use them in new build. By setting `VCPKG_BINARY_SOURCES=clear` in env., reusing cache can be disabled.

## Arrow Libs Pre-installation
Arrow libs are also pre-installed in docker, assuming they are not actively changed and not necessarily to be re-built every time.

## Updating Docker
Two Github secrets `DOCKERHUB_USER` & `DOCKERHUB_TOKEN` can be used to push docker to docker hub: https://hub.docker.com/r/apache/gluten/tags.
