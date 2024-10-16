FROM centos:7

RUN sed -i "s/enabled=1/enabled=0/" /etc/yum/pluginconf.d/fastestmirror.conf

WORKDIR /tmp/vcpkg
COPY vcpkg/setup-build-depends.sh setup-build-depends.sh
RUN ./setup-build-depends.sh

WORKDIR /
RUN rm -rf /tmp/vcpkg

ARG BUILDER_UID
ARG BUILDER_GID
RUN if ["$BUILDER_GID" != "0"]; then \
    groupadd -g ${BUILDER_GID} build; \
    fi
RUN if ["$BUILDER_UID" != "0"]; then \
    useradd -mu ${BUILDER_UID} -g ${BUILDER_GID} -G wheel build; \
    fi
USER ${BUILDER_UID}:${BUILDER_GID}

ENV VCPKG_BINARY_SOURCES=default
COPY docker/entrypoint.sh /entrypoint
ENTRYPOINT ["/entrypoint"]
CMD ["sleep", "inf"]
