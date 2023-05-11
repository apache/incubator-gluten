vcpkg_from_github(
        OUT_SOURCE_PATH SOURCE_PATH
        REPO apache/hawq
        REF dc6282fa4fe3613bec8694f8503c039db6bc3b66
        SHA512 661e5e6bf1c1296f78fad8fa33e72f98947c62beb36bd4d5f0efe75fb190c68c5354591ac2400deeaf2a0a15f6e25623a9064f61a7ec90d7ef0c4309781e7ef2
        HEAD_REF master
        PATCHES hdfs3.patch
)

vcpkg_configure_cmake(
        SOURCE_PATH ${SOURCE_PATH}/depends/libhdfs3
        PREFER_NINJA
)

vcpkg_install_cmake()

vcpkg_copy_pdbs()

file(GLOB HDFS3_SHARED_LIBS ${CURRENT_PACKAGES_DIR}/debug/lib/libhdfs3.so* ${CURRENT_PACKAGES_DIR}/lib/libhdfs3.so*)
file(REMOVE ${HDFS3_SHARED_LIBS})

file(REMOVE_RECURSE ${CURRENT_PACKAGES_DIR}/debug/include ${CURRENT_PACKAGES_DIR}/debug/share)
file(INSTALL ${SOURCE_PATH}/LICENSE DESTINATION ${CURRENT_PACKAGES_DIR}/share/${PORT} RENAME copyright)
FILE(INSTALL ${CMAKE_CURRENT_LIST_DIR}/libhdfs3Config.cmake DESTINATION ${CURRENT_PACKAGES_DIR}/share/${PORT})
FILE(INSTALL ${CMAKE_CURRENT_LIST_DIR}/usage DESTINATION ${CURRENT_PACKAGES_DIR}/share/${PORT})
