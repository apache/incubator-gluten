/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.utils

import org.apache.gluten.vectorized.JniLibLoader

class SharedLibraryLoaderUbuntu2004 extends SharedLibraryLoader {
  override def loadLib(loader: JniLibLoader): Unit = {
    def loadAndCreateLink(libName: String, linkName: String): Unit = {
      val mapLibName = System.mapLibraryName(libName)
      loader.loadAndCreateLink(mapLibName, linkName, false)
    }

    loadAndCreateLink("libroken.so.18", "libroken.so")
    loadAndCreateLink("libasn1.so.8", "libasn1.so")
    loadAndCreateLink("libboost_context.so.1.84.0", "libboost_context.so")
    loadAndCreateLink("libboost_regex.so.1.84.0", "libboost_regex.so")
    loadAndCreateLink("libbrotlicommon.so.1", "libbrotlicommon.so")
    loadAndCreateLink("libbrotlidec.so.1", "libbrotlidec.so")
    loadAndCreateLink("libpsl.so.5", "libpsl.so")
    loadAndCreateLink("libcrypto.so.1.1", "libcrypto.so")
    loadAndCreateLink("libnghttp2.so.14", "libnghttp2.so")
    loadAndCreateLink("libnettle.so.7", "libnettle.so")
    loadAndCreateLink("libhogweed.so.5", "libhogweed.so")
    loadAndCreateLink("librtmp.so.1", "librtmp.so")
    loadAndCreateLink("libssh.so.4", "libssh.so")
    loadAndCreateLink("libssl.so.1.1", "libssl.so")
    loadAndCreateLink("liblber-2.4.so.2", "liblber-2.4.so")
    loadAndCreateLink("libsasl2.so.2", "libsasl2.so")
    loadAndCreateLink("libwind.so.0", "libwind.so")
    loadAndCreateLink("libheimbase.so.1", "libheimbase.so")
    loadAndCreateLink("libhcrypto.so.4", "libhcrypto.so")
    loadAndCreateLink("libhx509.so.5", "libhx509.so")
    loadAndCreateLink("libkrb5.so.26", "libkrb5.so")
    loadAndCreateLink("libheimntlm.so.0", "libheimntlm.so")
    loadAndCreateLink("libgssapi.so.3", "libgssapi.so")
    loadAndCreateLink("libldap_r-2.4.so.2", "libldap_r-2.4.so")
    loadAndCreateLink("libcurl.so.4", "libcurl.so")
    loadAndCreateLink("libdouble-conversion.so.3", "libdouble-conversion.so")
    loadAndCreateLink("libevent-2.1.so.7", "libevent-2.1.so")
    loadAndCreateLink("libgflags.so.2.2", "libgflags.so")
    loadAndCreateLink("libunwind.so.8", "libunwind.so")
    loadAndCreateLink("libglog.so.0", "libglog.so")
    loadAndCreateLink("libidn.so.11", "libidn.so")
    loadAndCreateLink("libntlm.so.0", "libntlm.so")
    loadAndCreateLink("libgsasl.so.7", "libgsasl.so")
    loadAndCreateLink("libprotobuf.so.32", "libprotobuf.so")
    loadAndCreateLink("libicudata.so.66", "libicudata.so")
    loadAndCreateLink("libicuuc.so.66", "libicuuc.so")
    loadAndCreateLink("libxml2.so.2", "libxml2.so")
    loadAndCreateLink("libhdfs3.so.1", "libhdfs3.so")
    loadAndCreateLink("libre2.so.5", "libre2.so")
    loadAndCreateLink("libsnappy.so.1", "libsnappy.so")
    loadAndCreateLink("libthrift-0.13.0.so", "libthrift.so")
  }
}
