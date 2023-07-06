vcpkg_download_distfile(ARCHIVE
    URLS "https://gitlab.com/gsasl/libntlm/-/archive/libntlm-1-5/libntlm-libntlm-1-5.tar.gz"
    FILENAME "libntlm-libntlm-1-5.tar.gz"
    SHA512 8fa98c3ab3fe67fbb04c7806cd8af9bd4e8e38163f9eb59bfbb0c80fc3667bc49f50d917931d1eb4eb3664168e69b062246d098ed81f4ecd63d503829da5277e
)

vcpkg_extract_source_archive_ex(
    OUT_SOURCE_PATH SOURCE_PATH
    ARCHIVE "${ARCHIVE}"
)


vcpkg_configure_make(
    SOURCE_PATH ${SOURCE_PATH}
    OPTIONS
)
vcpkg_install_make()
vcpkg_fixup_pkgconfig()

# file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/share")
# file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/share")
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/tools")

vcpkg_install_copyright(FILE_LIST "${SOURCE_PATH}/COPYING")