FROM centos:7

RUN sed -i "s/enabled=1/enabled=0/" /etc/yum/pluginconf.d/fastestmirror.conf

WORKDIR /tmp/vcpkg
COPY setup-build-depends.sh setup-build-depends.sh
RUN ./setup-build-depends.sh

WORKDIR /
RUN rm -rf /tmp/vcpkg

ARG BUILDER_UID
ARG BUILDER_GID
RUN groupadd -g ${BUILDER_GID} build
RUN useradd -mu ${BUILDER_UID} -g build -G wheel build
USER ${BUILDER_UID}:${BUILDER_GID}

ENV VCPKG_BINARY_SOURCES=default
COPY docker/entrypoint.sh /entrypoint
ENTRYPOINT ["/entrypoint"]
CMD ["sleep", "inf"]