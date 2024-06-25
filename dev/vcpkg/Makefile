DOCKER_IMAGE_STABLE=apache/gluten:gluten-vcpkg-builder_2024_05_22
DOCKER_IMAGE_LOCAL=apache/gluten:gluten-vcpkg-builder_`date '+%Y_%m_%d'`
DOCKER_IMAGE=gluten-builder-vcpkg
GLUTEN_REPO=$(shell realpath -L ../..)

CCACHE_DIR=$(HOME)/.ccache
VCPKG_BINARY_CACHE_DIR=$(HOME)/.cache/vcpkg
MAVEN_M2_DIR=$(HOME)/.m2

$(info $(GLUTEN_REPO))

.PHONY: docker-image build docker-image-gha
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
		$(DOCKER_IMAGE_STABLE) \
		./dev/package-vcpkg-gha.sh

docker-image:
	docker build \
		--file docker/Dockerfile \
		--build-arg BUILDER_UID=`id -u` \
		--build-arg BUILDER_GID=`id -g` \
		--tag "$(DOCKER_IMAGE)" \
		.

docker-image-gha:
	docker build \
		--file docker/Dockerfile.gha \
		--tag "$(DOCKER_IMAGE_LOCAL)" \
		.


$(CCACHE_DIR) $(VCPKG_BINARY_CACHE_DIR) $(MAVEN_M2_DIR): %:
	mkdir -p $@
