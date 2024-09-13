# rebuild in #33610
# docker build -t libch_builder .
FROM ubuntu:20.04

# ARG for quick switch to a given ubuntu mirror
ARG apt_archive="http://mirrors.aliyun.com"
RUN sed -i "s|http://archive.ubuntu.com|$apt_archive|g" /etc/apt/sources.list

ENV DEBIAN_FRONTEND=noninteractive LLVM_VERSION=17

RUN apt-get update \
    && apt-get install \
        apt-transport-https \
        apt-utils \
        ca-certificates \
        curl \
        dnsutils \
        gnupg \
        iputils-ping \
        lsb-release \
        wget \
        python3 \
        yasm \
        nasm \
        gawk \
        --yes --no-install-recommends --verbose-versions \
    && export LLVM_PUBKEY_HASH="bda960a8da687a275a2078d43c111d66b1c6a893a3275271beedf266c1ff4a0cdecb429c7a5cccf9f486ea7aa43fd27f" \
    && wget -nv -O /tmp/llvm-snapshot.gpg.key https://apt.llvm.org/llvm-snapshot.gpg.key \
    && echo "${LLVM_PUBKEY_HASH} /tmp/llvm-snapshot.gpg.key" | sha384sum -c \
    && apt-key add /tmp/llvm-snapshot.gpg.key \
    && export CODENAME="$(lsb_release --codename --short | tr 'A-Z' 'a-z')" \
    && echo "deb https://apt.llvm.org/${CODENAME}/ llvm-toolchain-${CODENAME}-${LLVM_VERSION} main" >> \
        /etc/apt/sources.list \
    && apt-get clean

RUN curl -s https://apt.kitware.com/keys/kitware-archive-latest.asc | \
        gpg --dearmor - > /etc/apt/trusted.gpg.d/kitware.gpg && \
    echo "deb https://apt.kitware.com/ubuntu/ $(lsb_release -cs) main" >> /etc/apt/sources.list

# initial packages
RUN apt-get update \
    && apt-get install \
        bash \
        openjdk-8-jdk\
        ccache \
        clang-${LLVM_VERSION} \
        cmake \
        fakeroot \
        git \
        lld-${LLVM_VERSION} \
        llvm-${LLVM_VERSION} \
        ninja-build \
        software-properties-common \
        tzdata \
        --yes --no-install-recommends \
#       clang-tidy-${LLVM_VERSION} \
#       gperf \
#       gdb \
#       llvm-${LLVM_VERSION}-dev \
#       moreutils \
#       build-essential \
#       pigz \
#       rename \
    && apt-get clean

# This symlink required by gcc to find lld compiler
RUN ln -s /usr/bin/lld-${LLVM_VERSION} /usr/bin/ld.lld
ENV RUSTUP_HOME=/rust/rustup
ENV CARGO_HOME=/rust/cargo
ENV PATH="/rust/cargo/env:${PATH}"
ENV PATH="/rust/cargo/bin:${PATH}"
RUN curl https://sh.rustup.rs -sSf | bash -s -- -y && \
    chmod 777 -R /rust && \
    rustup target add aarch64-unknown-linux-gnu && \
    rustup target add x86_64-apple-darwin && \
    rustup target add x86_64-unknown-freebsd && \
    rustup target add aarch64-apple-darwin && \
    rustup target add powerpc64le-unknown-linux-gnu

ENV CC=clang-${LLVM_VERSION}
ENV CXX=clang++-${LLVM_VERSION}

ADD ./build.sh /build.sh
RUN chmod +x /build.sh
CMD ["bash", "-c", "/build.sh 2>&1"]
