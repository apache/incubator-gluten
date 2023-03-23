#!/bin/bash
CURRENT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
GLUTEN_DIR="$CURRENT_DIR/.."
THIRDPARTY_LIB="$GLUTEN_DIR/package/target/thirdparty-lib"
LINUX_OS=$(. /etc/os-release && echo ${ID})
VERSION=$(. /etc/os-release && echo ${VERSION_ID})

# compile gluten jar
./builddeps-veloxbe.sh --build_test=ON --build_benchmarks=ON --enable_s3=ON  --enable_hdfs=ON
mvn clean package -Pbackends-velox -Pspark-3.2 -DskipTests
mvn clean package -Pbackends-velox -Pspark-3.3 -DskipTests

mkdir -p $THIRDPARTY_LIB
function process_setup_ubuntu_2004 {
  cp /usr/lib/x86_64-linux-gnu/{libroken.so.18,libasn1.so.8,libboost_context.so.1.71.0,libboost_regex.so.1.71.0,libcrypto.so.1.1,libnghttp2.so.14,libnettle.so.7,libhogweed.so.5,librtmp.so.1,libssh.so.4,libssl.so.1.1,liblber-2.4.so.2,libsasl2.so.2,libwind.so.0,libheimbase.so.1,libhcrypto.so.4,libhx509.so.5,libkrb5.so.26,libheimntlm.so.0,libgssapi.so.3,libldap_r-2.4.so.2,libcurl.so.4,libdouble-conversion.so.3,libevent-2.1.so.7,libgflags.so.2.2,libunwind.so.8,libglog.so.0,libidn.so.11,libntlm.so.0,libgsasl.so.7,libicudata.so.66,libicuuc.so.66,libxml2.so.2,libre2.so.5,libsnappy.so.1,libpsl.so.5,libbrotlidec.so.1,libbrotlicommon.so.1} $THIRDPARTY_LIB/
  cp /usr/local/lib/{libprotobuf.so.32,libhdfs3.so.1} $THIRDPARTY_LIB/
}

if [ "$LINUX_OS" == "ubuntu" ]; then
  if [ "$VERSION" == "20.04" ]; then
    process_setup_ubuntu_2004
  elif [ "$VERSION" == "22.04" ]; then
    process_setup_ubuntu_2204
  fi
elif [ "$LINUX_OS" == "centos" ]; then
  echo $LINUX_OS-$VERSION
fi
cd $THIRDPARTY_LIB/
jar cvf gluten-thirdparty-lib-$LINUX_OS-$VERSION.jar ./
