vcpkg_download_distfile(ARCHIVE
    URLS "https://github.com/memkind/memkind/archive/refs/tags/v1.14.0.tar.gz"
    FILENAME "v1.14.0.tar.gz"
    SHA512 313c97c28be817abc86929565063859ec475bad31871a8ebed1f6ebca6b1c5f6b8c1d1ae947f694320b4a7c9998755681ac5d13a865ec8993440fcd8b32d7b94
)

vcpkg_extract_source_archive_ex(
    OUT_SOURCE_PATH SOURCE_PATH
    ARCHIVE "${ARCHIVE}"
    PATCHES disable-example.patch
)

vcpkg_configure_make(
    SOURCE_PATH ${SOURCE_PATH}
    COPY_SOURCE
)
vcpkg_install_make()

file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/share")
vcpkg_fixup_pkgconfig()
vcpkg_install_copyright(FILE_LIST "${SOURCE_PATH}/COPYING")