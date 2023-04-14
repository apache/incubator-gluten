package io.glutenproject.utils

import io.glutenproject.vectorized.JniLibLoader

class VeloxSharedlibraryLoaderCentos8 extends VeloxSharedlibraryLoader {
  override def loadLib(loader: JniLibLoader) : Unit = {
    loader.newTransaction()
      .loadAndCreateLink("libboost_thread.so.1.72.0", "libboost_thread.so", false)
      .loadAndCreateLink("libboost_system.so.1.72.0", "libboost_system.so", false)
      .loadAndCreateLink("libicudata.so.60", "libicudata.so", false)
      .loadAndCreateLink("libicuuc.so.60", "libicuuc.so", false)
      .loadAndCreateLink("libicui18n.so.60", "libicui18n.so", false)
      .loadAndCreateLink("libboost_regex.so.1.72.0", "libboost_regex.so", false)
      .loadAndCreateLink("libboost_program_options.so.1.72.0", "libboost_program_options.so", false)
      .loadAndCreateLink("libboost_filesystem.so.1.72.0", "libboost_filesystem.so", false)
      .loadAndCreateLink("libboost_context.so.1.72.0", "libboost_context.so", false)
      .loadAndCreateLink("libdouble-conversion.so.3", "libdouble-conversion.so", false)
      .loadAndCreateLink("libevent-2.1.so.6", "libevent-2.1.so", false)
      .loadAndCreateLink("libgflags.so.2.2", "libgflags.so", false)
      .loadAndCreateLink("libglog.so.0", "libglog.so", false)
      .loadAndCreateLink("libdwarf.so.1", "libdwarf.so", false)
      .loadAndCreateLink("libidn.so.11", "libidn.so", false)
      .loadAndCreateLink("libntlm.so.0", "libntlm.so", false)
      .loadAndCreateLink("libgsasl.so.7", "libgsasl.so", false)
      .loadAndCreateLink("libprotobuf.so.32", "libprotobuf.so", false)
      .loadAndCreateLink("libhdfs3.so.1", "libhdfs3.so", false)
      .loadAndCreateLink("libre2.so.0", "libre2.so", false)
      .commit()
  }
}

