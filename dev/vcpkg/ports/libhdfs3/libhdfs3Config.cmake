include(CMakeFindDependencyMacro)
include(FindPkgConfig)

find_dependency(Boost COMPONENTS thread chrono system atomic iostreams)
find_dependency(LibXml2)
find_dependency(Protobuf)
pkg_check_modules(Gsasl REQUIRED libgsasl mit-krb5-gssapi)
pkg_check_modules(UUID REQUIRED uuid)

FUNCTION(SET_LIBRARY_TARGET NAMESPACE LIB_NAME DEBUG_LIB_FILE_NAME RELEASE_LIB_FILE_NAME INCLUDE_DIR)
    ADD_LIBRARY(${NAMESPACE}::${LIB_NAME} STATIC IMPORTED)
    SET_TARGET_PROPERTIES(${NAMESPACE}::${LIB_NAME} PROPERTIES
                          IMPORTED_CONFIGURATIONS "RELEASE;DEBUG"
                          IMPORTED_LOCATION_RELEASE "${RELEASE_LIB_FILE_NAME}"
                          IMPORTED_LOCATION_DEBUG "${DEBUG_LIB_FILE_NAME}"
                          INTERFACE_INCLUDE_DIRECTORIES "${INCLUDE_DIR}"
                          INTERFACE_LINK_LIBRARIES "protobuf::libprotobuf;LibXml2::LibXml2;${Gsasl_LINK_LIBRARIES};${UUID_LINK_LIBRARIES}"
                          )
    SET(${NAMESPACE}_${LIB_NAME}_FOUND 1)
ENDFUNCTION()

GET_FILENAME_COMPONENT(ROOT "${CMAKE_CURRENT_LIST_FILE}" PATH)
GET_FILENAME_COMPONENT(ROOT "${ROOT}" PATH)
GET_FILENAME_COMPONENT(ROOT "${ROOT}" PATH)

SET_LIBRARY_TARGET("HDFS" "hdfs3" "${ROOT}/debug/lib/libhdfs3.a" "${ROOT}/lib/libhdfs3.a" "${ROOT}/include/hdfs")