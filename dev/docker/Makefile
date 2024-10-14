DOCKER_IMAGE=apache/gluten:vcpkg-centos-7
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
		-ti \
		$(DOCKER_IMAGE) \
		./dev/package-vcpkg.sh

docker-image:
	docker build \
		--file Dockerfile \
		--build-arg BUILDER_UID=`id -u` \
		--build-arg BUILDER_GID=`id -g` \
		--tag "$(DOCKER_IMAGE)" \
		.

docker-image-static:
	docker build \
		--file Dockerfile.centos7-static-build \
		--tag "$(DOCKER_IMAGE)" \
		--build-arg HTTPS_PROXY="" \
		--build-arg HTTP_PROXY="" \
		.

docker-image-dynamic:
	docker build \
		--file Dockerfile.centos8-dynamic-build \
		--tag "$(DOCKER_IMAGE)" \
		--build-arg HTTPS_PROXY="" \
		--build-arg HTTP_PROXY="" \
		.

$(CCACHE_DIR) $(VCPKG_BINARY_CACHE_DIR) $(MAVEN_M2_DIR): %:
	mkdir -p $@
