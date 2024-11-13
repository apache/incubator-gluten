vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO jemalloc/jemalloc
    REF 54eaed1d8b56b1aa528be3bdd1877e59c56fa90c
    SHA512 527bfbf5db9a5c2b7b04df4785b6ae9d445cff8cb17298bf3e550c88890d2bd7953642d8efaa417580610508279b527d3a3b9e227d17394fd2013c88cb7ae75a
    HEAD_REF master
    PATCHES
        fix-configure-ac.patch
        preprocessor.patch
)

set(opts
  # The setting for prefix can be helpful to explicitly use jemalloc for some code, not targeting
  # for global replacement.
  # "--with-jemalloc-prefix=je_gluten_"
  # "--with-private-namespace=je_gluten_private_"
  # "--without-export"
  # "--disable-shared"
  "--disable-cxx"
  "--disable-libdl"
  # For fixing an issue when loading native lib: cannot allocate memory in static TLS block.
  "--disable-initial-exec-tls"
  "CFLAGS=-fPIC"
  "CXXFLAGS=-fPIC")
vcpkg_configure_make(
    SOURCE_PATH "${SOURCE_PATH}"
    AUTOCONFIG
    NO_WRAPPERS
    OPTIONS ${opts}
)

vcpkg_install_make()

vcpkg_fixup_pkgconfig()

vcpkg_copy_pdbs()

file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/include")
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/share")
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/tools")

# Handle copyright
file(INSTALL "${SOURCE_PATH}/COPYING" DESTINATION "${CURRENT_PACKAGES_DIR}/share/${PORT}" RENAME copyright)
