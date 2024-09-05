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

class SharedLibraryLoaderCentos7 extends SharedLibraryLoader {
  override def loadLib(loader: JniLibLoader): Unit = {
    def loadAndCreateLink(libName: String, linkName: String): Unit = {
      val mapLibName = System.mapLibraryName(libName)
      loader.loadAndCreateLink(mapLibName, linkName, false)
    }

    loadAndCreateLink("libboost_atomic.so.1.84.0", "libboost_atomic.so")
    loadAndCreateLink("libboost_thread.so.1.84.0", "libboost_thread.so")
    loadAndCreateLink("libboost_system.so.1.84.0", "libboost_system.so")
    loadAndCreateLink("libboost_regex.so.1.84.0", "libboost_regex.so")
    loadAndCreateLink("libboost_program_options.so.1.84.0", "libboost_program_options.so")
    loadAndCreateLink("libboost_filesystem.so.1.84.0", "libboost_filesystem.so")
    loadAndCreateLink("libboost_context.so.1.84.0", "libboost_context.so")
    loadAndCreateLink("libdouble-conversion.so.1", "libdouble-conversion.so")
    loadAndCreateLink("libevent-2.0.so.5", "libevent-2.0.so")
    loadAndCreateLink("libgflags.so.2.2", "libgflags.so")
    loadAndCreateLink("libglog.so.0", "libglog.so")
    loadAndCreateLink("libntlm.so.0", "libntlm.so")
    loadAndCreateLink("libgsasl.so.7", "libgsasl.so")
    loadAndCreateLink("libprotobuf.so.32", "libprotobuf.so")
    loadAndCreateLink("libhdfs3.so.1", "libhdfs3.so")
    loadAndCreateLink("libre2.so.10", "libre2.so")
    loadAndCreateLink("libzstd.so.1", "libzstd.so")
    loadAndCreateLink("liblz4.so.1", "liblz4.so")
  }
}
