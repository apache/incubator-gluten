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
