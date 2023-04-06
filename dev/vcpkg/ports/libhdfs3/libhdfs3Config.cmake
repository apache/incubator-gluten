find_dependency(Boost COMPONENTS thread chrono system atomic iostreams)
find_dependency(LibXml2)
find_dependency(Protobuf)


FUNCTION(SET_LIBRARY_TARGET NAMESPACE LIB_NAME DEBUG_LIB_FILE_NAME RELEASE_LIB_FILE_NAME INCLUDE_DIR)
    ADD_LIBRARY(${NAMESPACE}::${LIB_NAME} STATIC IMPORTED)
    SET_TARGET_PROPERTIES(${NAMESPACE}::${LIB_NAME} PROPERTIES
                          IMPORTED_CONFIGURATIONS "RELEASE;DEBUG"
                          IMPORTED_LOCATION_RELEASE "${RELEASE_LIB_FILE_NAME}"
                          IMPORTED_LOCATION_DEBUG "${DEBUG_LIB_FILE_NAME}"
                          INTERFACE_INCLUDE_DIRECTORIES "${INCLUDE_DIR}"
                          INTERFACE_LINK_LIBRARIES "${PROTOBUF_LIBRARIES};LibXml2::LibXml2;${HDFS_DEPS}"
                          )
    SET(${NAMESPACE}_${LIB_NAME}_FOUND 1)
ENDFUNCTION()

GET_FILENAME_COMPONENT(ROOT "${CMAKE_CURRENT_LIST_FILE}" PATH)
GET_FILENAME_COMPONENT(ROOT "${ROOT}" PATH)
GET_FILENAME_COMPONENT(ROOT "${ROOT}" PATH)

# TODO: Use find_dependency()
SET(HDFS_DEPS
    uuid
    # krb5
    krb5
    k5crypto
    krb5support
    com_err
    # gsasl
    gsasl
    gssapi_krb5
)
list(TRANSFORM HDFS_DEPS PREPEND "${ROOT}/lib/lib")
list(TRANSFORM HDFS_DEPS APPEND ".a")

# Required by krb5
list(APPEND HDFS_DEPS "resolv")

SET_LIBRARY_TARGET("HDFS" "hdfs3" "${ROOT}/debug/lib/libhdfs3.a" "${ROOT}/lib/libhdfs3.a" "${ROOT}/include/hdfs")