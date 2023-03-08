
set(ARROW_INCLUDE_DIR "${CH_SOURCE_DIR}/contrib/arrow/cpp/src")


macro(add_headers_and_sources_including_cc prefix common_path)
    add_glob(${prefix}_headers . ${common_path}/*.h)
    add_glob(${prefix}_sources ${common_path}/*.cpp ${common_path}/*.c ${common_path}/*.cc ${common_path}/*.h)
endmacro()

add_headers_and_sources(substait_source .)
add_headers_and_sources_including_cc(ch_parquet arrow)
add_library(substait_source ${substait_source_sources})
target_compile_options(substait_source PUBLIC -fPIC
        -Wno-shorten-64-to-32
        -Wno-shadow-field-in-constructor
        -Wno-return-type
        -Wno-reserved-identifier
        -Wno-extra-semi-stmt
        -Wno-extra-semi
        -Wno-unused-result
        -Wno-unreachable-code-return
        -Wno-unused-parameter
        -Wno-unreachable-code
        -Wno-pessimizing-move
        -Wno-unreachable-code-break
        -Wno-unused-variable
        -Wno-inconsistent-missing-override
        -Wno-shadow-uncaptured-local
        -Wno-suggest-override
        -Wno-unused-member-function
        -Wno-deprecated-this-capture
        -Wno-unused-but-set-variable
        -Wno-sign-compare
        -Wno-implicit-const-int-float-conversion
        -Wno-unknown-warning-option
)

target_link_libraries(substait_source PUBLIC
    substrait
)

target_include_directories(substait_source SYSTEM BEFORE PUBLIC 
    ${ARROW_INCLUDE_DIR}
    ${ClickHouse_BINARY_DIR}/contrib/arrow-cmake/cpp/src
    ${CH_SOURCE_DIR}/contrib/arrow-cmake/cpp/src
)
