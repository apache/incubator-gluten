FROM centos:7

RUN sed -i "s/enabled=1/enabled=0/" /etc/yum/pluginconf.d/fastestmirror.conf

WORKDIR /tmp/vcpkg
COPY setup-build-depends.sh setup-build-depends.sh
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

ARG SSH_PRV_KEY
ARG SSH_PUB_KEY
RUN mkdir -p ~/.ssh && \
    chmod 0700 ~/.ssh
RUN if [[ -n "$SSH_PRV_KEY" ]] ; then \
    echo "$SSH_PRV_KEY" > ~/.ssh/id_rsa && \
    chmod 600 ~/.ssh/id_rsa && \
    echo -e "Host *\n    StrictHostKeyChecking no" > ~/.ssh/config && \
    chmod 600 ~/.ssh/config; \
    fi
RUN if [[ -n "$SSH_PUB_KEY" ]] ; then \
    echo "$SSH_PUB_KEY" > ~/.ssh/id_rsa.pub && \
    chmod 600 ~/.ssh/id_rsa.pub; \
    fi

ENV VCPKG_BINARY_SOURCES=default
COPY docker/entrypoint.sh /entrypoint
ENTRYPOINT ["/entrypoint"]
CMD ["sleep", "inf"]