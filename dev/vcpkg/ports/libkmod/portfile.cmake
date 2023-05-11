set(VCPKG_LIBRARY_LINKAGE dynamic)

vcpkg_download_distfile(ARCHIVE
    URLS "https://mirrors.edge.kernel.org/pub/linux/utils/kernel/kmod/kmod-30.tar.xz"
    FILENAME "kmod-30.tar.xz"
    SHA512 e2cd34e600a72e44710760dfda9364b790b8352a99eafbd43e683e4a06f37e6b5c0b5d14e7c28070e30fc5fc6ceddedf7b97f3b6c2c5c2d91204fefd630b9a3e
)

vcpkg_extract_source_archive_ex(
    OUT_SOURCE_PATH SOURCE_PATH
    ARCHIVE "${ARCHIVE}"
)

vcpkg_configure_make(SOURCE_PATH ${SOURCE_PATH})
vcpkg_install_make()

vcpkg_install_copyright(FILE_LIST "${SOURCE_PATH}/COPYING")
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/share")
vcpkg_fixup_pkgconfig()