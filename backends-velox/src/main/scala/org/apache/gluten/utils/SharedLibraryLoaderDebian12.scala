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

class SharedLibraryLoaderDebian12 extends SharedLibraryLoader {
  override def loadLib(loader: JniLibLoader): Unit = {
    loader
      .newTransaction()
      .loadAndCreateLink("libcrypto.so.3", "libcrypto.so", false)
      .loadAndCreateLink("libkrb5support.so.0", "libkrb5support.so", false)
      .loadAndCreateLink("libssl.so.3", "libssl.so", false)
      .loadAndCreateLink("libicudata.so.72", "libicudata.so", false)
      .loadAndCreateLink("libk5crypto.so.3", "libk5crypto.so", false)
      .loadAndCreateLink("libkeyutils.so.1", "libkeyutils.so", false)
      .loadAndCreateLink("libsnappy.so.1", "libsnappy.so", false)
      .loadAndCreateLink("libthrift-0.17.0.so", "libthrift.so", false)
      .loadAndCreateLink("libicuuc.so.72", "libicuuc.so", false)
      .loadAndCreateLink("libkrb5.so.3", "libkrb5.so", false)
      .loadAndCreateLink("liblber-2.5.so.0", "liblber-2.4.so", false)
      .loadAndCreateLink("libsasl2.so.2", "libsasl2.so", false)
      .loadAndCreateLink("libbrotlicommon.so.1", "libbrotlicommon.so", false)
      .loadAndCreateLink("libicui18n.so.72", "libicui18n.so", false)
      .loadAndCreateLink("libgflags.so.2.2", "libgflags.so", false)
      .loadAndCreateLink("libunwind.so.8", "libunwind.so", false)
      .loadAndCreateLink("libnghttp2.so.14", "libnghttp2.so", false)
      .loadAndCreateLink("librtmp.so.1", "librtmp.so", false)
      .loadAndCreateLink("libssh2.so.1", "libssh2.so", false)
      .loadAndCreateLink("libpsl.so.5", "libpsl.so", false)
      .loadAndCreateLink("libgssapi_krb5.so.2", "libgssapi_krb5.so", false)
      .loadAndCreateLink("libldap-2.5.so.0", "libldap_r-2.4.so", false)
      .loadAndCreateLink("libbrotlidec.so.1", "libbrotlidec.so", false)
      .loadAndCreateLink("libboost_context.so.1.84.0", "libboost_context.so", false)
      .loadAndCreateLink("libboost_regex.so.1.84.0", "libboost_regex.so", false)
      .loadAndCreateLink("libdouble-conversion.so.3", "libdouble-conversion.so", false)
      .loadAndCreateLink("libglog.so.1", "libglog.so", false)
      .loadAndCreateLink("libevent-2.1.so.7", "libevent-2.1.so", false)
      .loadAndCreateLink("libcurl.so.4", "libcurl.so", false)
      .loadAndCreateLink("libprotobuf.so.32", "libprotobuf.so", false)
      .loadAndCreateLink("libhdfs3.so.1", "libhdfs3.so", false)
      .commit()
  }
}
