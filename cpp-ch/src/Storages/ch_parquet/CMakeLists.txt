set(ARROW_INCLUDE_DIR "${CH_SOURCE_DIR}/contrib/arrow/cpp/src")

macro(add_headers_and_sources_including_cc prefix common_path)
    add_glob(${prefix}_headers . ${common_path}/*.h)
    add_glob(${prefix}_sources ${common_path}/*.cpp ${common_path}/*.c ${common_path}/*.cc ${common_path}/*.h)
endmacro()

add_headers_and_sources(ch_parquet .)
add_headers_and_sources_including_cc(ch_parquet arrow)

add_library(ch_parquet ${ch_parquet_sources})

target_compile_options(ch_parquet PUBLIC -fPIC
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
        -Wno-deprecated-builtins
        -Wno-sign-compare
        -Wno-deprecated-declarations
        -Wno-implicit-const-int-float-conversion
        -Wno-unknown-warning-option
)

target_link_libraries(ch_parquet PRIVATE
    cxx
    cxxabi
)

target_include_directories(ch_parquet SYSTEM BEFORE PUBLIC
    ${CH_BINARY_DIR}/contrib/arrow-cmake/cpp/src
    ${CH_SOURCE_DIR}/contrib/arrow-cmake/cpp/src
    ${ARROW_INCLUDE_DIR}
        )