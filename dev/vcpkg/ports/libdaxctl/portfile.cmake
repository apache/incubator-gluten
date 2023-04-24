vcpkg_download_distfile(ARCHIVE
    URLS "https://github.com/pmem/ndctl/archive/refs/tags/v76.1.tar.gz"
    FILENAME "v76.1.tar.gz"
    SHA512 76d32599df029969734276f8972f3f4bf701e471117c8a48d1f96b62c87a59ac54d59104ee62d1cbbb518a06a779677ca856df32ce6218d758a8c73daa3e5b06
)

vcpkg_extract_source_archive_ex(
    OUT_SOURCE_PATH SOURCE_PATH
    ARCHIVE "${ARCHIVE}"
    PATCHES fix-meson.patch
)

vcpkg_configure_meson(
    SOURCE_PATH ${SOURCE_PATH}
    OPTIONS
        -Dlibtracefs=disabled
        -Ddocs=disabled
        -Dsystemd=disabled
        -Dkeyutils=disabled
    OPTIONS_RELEASE
        -Drootprefix=${CURRENT_PACKAGES_DIR}
    OPTIONS_DEBUG
        -Drootprefix=${CURRENT_PACKAGES_DIR}/debug
)

vcpkg_install_meson()

file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/include")
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/share")
vcpkg_install_copyright(FILE_LIST "${SOURCE_PATH}/LICENSES/preferred/LGPL-2.1")
vcpkg_fixup_pkgconfig()