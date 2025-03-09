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

DOCKER_IMAGE=apache/gluten:centos-7-vcpkg-build-depends
GLUTEN_REPO=$(shell realpath -L ../..)

CCACHE_DIR=$(HOME)/.ccache
VCPKG_BINARY_CACHE_DIR=$(HOME)/.cache/vcpkg
MAVEN_M2_DIR=$(HOME)/.m2

$(info $(GLUTEN_REPO))

.PHONY: docker-image build
build: $(CCACHE_DIR) $(VCPKG_BINARY_CACHE_DIR) $(MAVEN_M2_DIR)
	docker run --rm -ti \
		-v $(GLUTEN_REPO):$(GLUTEN_REPO) \
		-v $(VCPKG_BINARY_CACHE_DIR):/root/.cache/vcpkg \
		-v $(MAVEN_M2_DIR):/root/.m2 \
		-v $(CCACHE_DIR):/root/.ccache \
		-e http_proxy \
		-e https_proxy \
		--workdir $(GLUTEN_REPO) \
		$(DOCKER_IMAGE) \
		./dev/package-vcpkg.sh

docker-image:
	docker build \
		--file Dockerfile \
		--build-arg BUILDER_UID=`id -u` \
		--build-arg BUILDER_GID=`id -g` \
		--tag "$(DOCKER_IMAGE)" \
		..

docker-image-static-build:
	docker build \
		--file Dockerfile.centos7-static-build \
		--tag "apache/gluten:vcpkg-centos-7" \
		--build-arg HTTPS_PROXY="" \
		--build-arg HTTP_PROXY="" \
		.

docker-image-dynamic-build:
	docker build \
		--file Dockerfile.centos8-dynamic-build \
		--tag "apache/gluten:centos-8-jdk8" \
		--build-arg HTTPS_PROXY="" \
		--build-arg HTTP_PROXY="" \
		.

$(CCACHE_DIR) $(VCPKG_BINARY_CACHE_DIR) $(MAVEN_M2_DIR): %:
	mkdir -p $@
