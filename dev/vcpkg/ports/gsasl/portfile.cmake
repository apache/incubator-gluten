vcpkg_download_distfile(ARCHIVE
    URLS "https://ftp.gnu.org/gnu/gsasl/gsasl-2.2.0.tar.gz" "https://www.mirrorservice.org/sites/ftp.gnu.org/gnu/gsasl/gsasl-2.2.0.tar.gz"
    FILENAME "gsasl-2.2.0.tar.gz"
    SHA512 0ae318a8616fe675e9718a3f04f33731034f9a7ba03d83ccb1a72954ded54ced35dc7c7e173fdcb6fa0f0813f8891c6cbcedf8bf70b37d00b8ec512eb9f07f5f
)

vcpkg_extract_source_archive_ex(
    OUT_SOURCE_PATH SOURCE_PATH
    ARCHIVE "${ARCHIVE}"
    PATCHES fix-krb5-config.patch
)

vcpkg_configure_make(
    SOURCE_PATH ${SOURCE_PATH}
    OPTIONS
        # --disable-ntlm
        --without-stringprep
        --with-gssapi-impl=mit
)
vcpkg_install_make()
vcpkg_fixup_pkgconfig()

file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/share")
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/share")
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/tools")

vcpkg_install_copyright(FILE_LIST "${SOURCE_PATH}/COPYING")