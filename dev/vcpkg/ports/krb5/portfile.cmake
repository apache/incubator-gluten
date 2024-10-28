vcpkg_download_distfile(ARCHIVE
    URLS "https://kerberos.org/dist/krb5/1.20/krb5-1.20.tar.gz"
    FILENAME "krb5-1.20.tar.gz"
    SHA512 9aed84a971a4d74188468870260087ec7c3a614cceb5fe32ad7da1cb8db3d66e00df801c9f900f0131ac56eb828674b8be93df474c2d13b892b70c7977388604
)

# for gcc >= 10
set(VCPKG_C_FLAGS "${VCPKG_C_FLAGS} -fcommon")

vcpkg_extract_source_archive_ex(
    OUT_SOURCE_PATH SOURCE_PATH
    ARCHIVE "${ARCHIVE}"
    PATCHES fix-pkgconfig.patch
)

vcpkg_configure_make(
    SOURCE_PATH ${SOURCE_PATH}/src
    OPTIONS --without-keyutils
)
vcpkg_install_make()

file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/include")
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/tools")

foreach(KRB5_SUBDIR_TO_REMOVE bin share lib/krb5 var)
  file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/${KRB5_SUBDIR_TO_REMOVE}")
  file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/${KRB5_SUBDIR_TO_REMOVE}")
endforeach()

vcpkg_install_copyright(FILE_LIST "${SOURCE_PATH}/doc/copyright.rst")
vcpkg_fixup_pkgconfig()