vcpkg_download_distfile(ARCHIVE
    URLS
        "https://github.com/davea42/libdwarf-code/releases/download/v0.6.0/libdwarf-0.6.0.tar.xz"
        "https://www.prevanders.net/libdwarf-0.6.0.tar.xz"
    FILENAME "libdwarf-0.6.0.tar.xz"
    SHA512 839ba5e4162630ad804d76bd2aa86f35780a178dcda110106a5ee4fb27807fdf45f12e8bbb399ff53721121d0169a73335898f94218a1853116bb106dd455950
)

vcpkg_extract_source_archive_ex(
    OUT_SOURCE_PATH SOURCE_PATH
    ARCHIVE "${ARCHIVE}"
)

vcpkg_cmake_configure(SOURCE_PATH "${SOURCE_PATH}")

vcpkg_cmake_install()
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/include")
vcpkg_cmake_config_fixup(CONFIG_PATH lib/cmake/libdwarf)
vcpkg_install_copyright(FILE_LIST "${SOURCE_PATH}/COPYING")
vcpkg_fixup_pkgconfig()