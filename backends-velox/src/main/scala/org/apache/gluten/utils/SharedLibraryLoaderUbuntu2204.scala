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

class SharedLibraryLoaderUbuntu2204 extends SharedLibraryLoader {
  override def loadLib(loader: JniLibLoader): Unit = {
    loader
      .newTransaction()
      .loadAndCreateLink("libboost_context.so.1.84.0", "libboost_context.so", false)
      .loadAndCreateLink("libicudata.so.70", "libicudata.so", false)
      .loadAndCreateLink("libicuuc.so.70", "libicuuc.so", false)
      .loadAndCreateLink("libicui18n.so.70", "libicui18n.so", false)
      .loadAndCreateLink("libboost_regex.so.1.84.0", "libboost_regex.so", false)
      .loadAndCreateLink("libnghttp2.so.14", "libnghttp2.so", false)
      .loadAndCreateLink("librtmp.so.1", "librtmp.so", false)
      .loadAndCreateLink("libssh.so.4", "libssh.so", false)
      .loadAndCreateLink("libsasl2.so.2", "libsasl2.so", false)
      .loadAndCreateLink("liblber-2.5.so.0", "liblber-2.5.so", false)
      .loadAndCreateLink("libldap-2.5.so.0", "libldap-2.5.so", false)
      .loadAndCreateLink("libcurl.so.4", "libcurl.so", false)
      .loadAndCreateLink("libdouble-conversion.so.3", "libdouble-conversion.so", false)
      .loadAndCreateLink("libevent-2.1.so.7", "libevent-2.1.so", false)
      .loadAndCreateLink("libgflags.so.2.2", "libgflags.so", false)
      .loadAndCreateLink("libunwind.so.8", "libunwind.so", false)
      .loadAndCreateLink("libglog.so.0", "libglog.so", false)
      .loadAndCreateLink("libidn.so.12", "libidn.so", false)
      .loadAndCreateLink("libntlm.so.0", "libntlm.so", false)
      .loadAndCreateLink("libgsasl.so.7", "libgsasl.so", false)
      .loadAndCreateLink("libprotobuf.so.32", "libprotobuf.so", false)
      .loadAndCreateLink("libxml2.so.2", "libxml2.so", false)
      .loadAndCreateLink("libhdfs3.so.1", "libhdfs3.so", false)
      .loadAndCreateLink("libre2.so.9", "libre2.so", false)
      .loadAndCreateLink("libsnappy.so.1", "libsnappy.so", false)
      .loadAndCreateLink("libthrift-0.16.0.so", "libthrift.so", false)
      .commit()
  }
}
