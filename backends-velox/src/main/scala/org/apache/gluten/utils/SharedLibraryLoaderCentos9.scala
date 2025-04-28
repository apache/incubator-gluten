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

class SharedLibraryLoaderCentos9 extends SharedLibraryLoader {
  override def loadLib(loader: JniLibLoader): Unit = {
    loader.loadAndCreateLink("libboost_atomic.so.1.84.0", "libboost_atomic.so")
    loader.loadAndCreateLink("libboost_thread.so.1.84.0", "libboost_thread.so")
    loader.loadAndCreateLink("libboost_system.so.1.84.0", "libboost_system.so")
    loader.loadAndCreateLink("libicudata.so.67", "libicudata.so")
    loader.loadAndCreateLink("libicuuc.so.67", "libicuuc.so")
    loader.loadAndCreateLink("libicui18n.so.67", "libicui18n.so")
    loader.loadAndCreateLink("libboost_regex.so.1.84.0", "libboost_regex.so")
    loader.loadAndCreateLink("libboost_program_options.so.1.84.0", "libboost_program_options.so")
    loader.loadAndCreateLink("libboost_filesystem.so.1.84.0", "libboost_filesystem.so")
    loader.loadAndCreateLink("libboost_context.so.1.84.0", "libboost_context.so")
    loader.loadAndCreateLink("libdouble-conversion.so.3", "libdouble-conversion.so")
    loader.loadAndCreateLink("libevent-2.1.so.7", "libevent-2.1.so")
    loader.loadAndCreateLink("libgflags.so.2.2", "libgflags.so")
    loader.loadAndCreateLink("libglog.so.1", "libglog.so")
    loader.loadAndCreateLink("libdwarf.so.0", "libdwarf.so")
    loader.loadAndCreateLink("libidn.so.12", "libidn.so")
    loader.loadAndCreateLink("libntlm.so.0", "libntlm.so")
    loader.loadAndCreateLink("libgsasl.so.7", "libgsasl.so")
    loader.loadAndCreateLink("libprotobuf.so.32", "libprotobuf.so")
    loader.loadAndCreateLink("libre2.so.9", "libre2.so")
    loader.loadAndCreateLink("libsodium.so.23", "libsodium.so")
  }
}
