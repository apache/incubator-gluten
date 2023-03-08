set(LIBCXXABI_SOURCE_DIR "${CH_SOURCE_DIR}/contrib/llvm-project/libcxxabi")

add_library(cxxabi STATIC IMPORTED GLOBAL)
set_target_properties(cxxabi PROPERTIES IMPORTED_LOCATION ${CH_BINARY_DIR}/contrib/libcxxabi-cmake/libcxxabi.a)

target_include_directories(cxxabi SYSTEM BEFORE INTERFACE
         $<BUILD_INTERFACE:${LIBCXXABI_SOURCE_DIR}/include>
        )
target_compile_definitions(cxxabi INTERFACE -DSTD_EXCEPTION_HAS_STACK_TRACE=1)
