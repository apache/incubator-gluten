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

import org.apache.gluten.jni.JniLibLoader

class SharedLibraryLoaderCentos7 extends SharedLibraryLoader {
  override def loadLib(loader: JniLibLoader): Unit = {
    loader.loadAndCreateLink("libboost_atomic.so.1.84.0", "libboost_atomic.so", false)
    loader.loadAndCreateLink("libboost_thread.so.1.84.0", "libboost_thread.so", false)
    loader.loadAndCreateLink("libboost_system.so.1.84.0", "libboost_system.so", false)
    loader.loadAndCreateLink("libboost_regex.so.1.84.0", "libboost_regex.so", false)
    loader.loadAndCreateLink(
      "libboost_program_options.so.1.84.0",
      "libboost_program_options.so",
      false)
    loader.loadAndCreateLink("libboost_filesystem.so.1.84.0", "libboost_filesystem.so", false)
    loader.loadAndCreateLink("libboost_context.so.1.84.0", "libboost_context.so", false)
    loader.loadAndCreateLink("libdouble-conversion.so.1", "libdouble-conversion.so", false)
    loader.loadAndCreateLink("libevent-2.0.so.5", "libevent-2.0.so", false)
    loader.loadAndCreateLink("libgflags.so.2.2", "libgflags.so", false)
    loader.loadAndCreateLink("libglog.so.0", "libglog.so", false)
    loader.loadAndCreateLink("libntlm.so.0", "libntlm.so", false)
    loader.loadAndCreateLink("libgsasl.so.7", "libgsasl.so", false)
    loader.loadAndCreateLink("libprotobuf.so.32", "libprotobuf.so", false)
    loader.loadAndCreateLink("libhdfs.so.0.0.0", "libhdfs.so", false)
    loader.loadAndCreateLink("libre2.so.10", "libre2.so", false)
    loader.loadAndCreateLink("libzstd.so.1", "libzstd.so", false)
    loader.loadAndCreateLink("liblz4.so.1", "liblz4.so", false)
  }
}
