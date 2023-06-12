vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO ClickHouse/libhdfs3
    HEAD_REF master
    REF 164b89253fad7991bce77882f01b51ab81d19f3d
    SHA512 147884e82afc5726e9eff383dc227ec5ff781c7b4c6ceec32bf3fff332e7e4e0304047282cc55de52b4e6cb082e958073f60add1be0b8f2792be753acc0d68d1
    PATCHES
        fix-yasm.patch
        default-socket-path.patch
        default-user.patch
)

vcpkg_configure_cmake(
    SOURCE_PATH ${SOURCE_PATH}
    PREFER_NINJA
    OPTIONS
        -DCMAKE_PROGRAM_PATH=${CURRENT_HOST_INSTALLED_DIR}/tools/yasm
        -DWITH_KERBEROS=on
)

vcpkg_install_cmake()

vcpkg_copy_pdbs()

file(GLOB HDFS3_SHARED_LIBS ${CURRENT_PACKAGES_DIR}/debug/lib/libhdfs3.so* ${CURRENT_PACKAGES_DIR}/lib/libhdfs3.so*)
file(REMOVE ${HDFS3_SHARED_LIBS})

file(REMOVE_RECURSE ${CURRENT_PACKAGES_DIR}/debug/include ${CURRENT_PACKAGES_DIR}/debug/share)
file(INSTALL ${SOURCE_PATH}/LICENSE.txt DESTINATION ${CURRENT_PACKAGES_DIR}/share/${PORT} RENAME copyright)
FILE(INSTALL ${CMAKE_CURRENT_LIST_DIR}/libhdfs3Config.cmake DESTINATION ${CURRENT_PACKAGES_DIR}/share/${PORT})
FILE(INSTALL ${CMAKE_CURRENT_LIST_DIR}/usage DESTINATION ${CURRENT_PACKAGES_DIR}/share/${PORT})
