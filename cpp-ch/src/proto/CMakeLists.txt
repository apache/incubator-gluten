set(PROTO_SOURCE ${CMAKE_SOURCE_DIR}/../gluten-core/src/main/resources/substrait/proto)
file(GLOB protobuf_files
        ${PROTO_SOURCE}/substrait/*.proto
        ${PROTO_SOURCE}/substrait/extensions/*.proto
        )
FOREACH(FIL ${protobuf_files})
    file(RELATIVE_PATH FIL_RELATIVE ${PROTO_SOURCE} ${FIL})
    string(REPLACE "\\.proto" "" FILE_NAME ${FIL_RELATIVE})
    string(REPLACE "../.." "" FILE_NAME ${FILE_NAME})
    string(REPLACE "cpp-ch/local-engine/proto/" "" FILE_NAME ${FILE_NAME})
    string(REPLACE ".proto" "" FILE_NAME ${FILE_NAME})
    LIST(APPEND PROTO_SOURCE_LIST ${FIL_RELATIVE})
    LIST(APPEND SUBSTRAIT_SRCS "${CMAKE_CURRENT_BINARY_DIR}/${FILE_NAME}.pb.cc")
    LIST(APPEND SUBSTRAIT_HEADERS "${CMAKE_CURRENT_BINARY_DIR}/${FILE_NAME}.pb.h")
ENDFOREACH()

add_custom_target(
        generate_substrait
        COMMAND ${CH_BINARY_DIR}/contrib/protobuf-cmake/protoc -I${PROTO_SOURCE} -I${CH_SOURCE_DIR}/contrib/protobuf/src --cpp_out=${CMAKE_CURRENT_BINARY_DIR}/ ${PROTO_SOURCE_LIST}
        COMMENT "Running cpp protocol buffer compiler"
        VERBATIM )

set(Protobuf_INCLUDE_DIR "${CH_SOURCE_DIR}/contrib/protobuf/src")

set_source_files_properties(${SUBSTRAIT_SRCS} PROPERTIES GENERATED TRUE)

add_library(substrait ${SUBSTRAIT_SRCS})
target_compile_options(substrait PUBLIC -fPIC -Wno-reserved-identifier -Wno-deprecated)
add_dependencies(substrait generate_substrait)
target_include_directories(substrait SYSTEM BEFORE PUBLIC ${CMAKE_CURRENT_BINARY_DIR} ${Protobuf_INCLUDE_DIR})
target_link_libraries(substrait PRIVATE ${THIRD_PARTY})

