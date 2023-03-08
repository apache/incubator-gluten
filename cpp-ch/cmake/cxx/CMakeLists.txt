set(LIBCXX_SOURCE_DIR "${CH_SOURCE_DIR}/contrib/llvm-project/libcxx")

add_library(cxx STATIC IMPORTED GLOBAL)
set_target_properties(cxx PROPERTIES IMPORTED_LOCATION ${CH_BINARY_DIR}/contrib/libcxx-cmake/libcxx.a)

target_include_directories(cxx SYSTEM BEFORE PUBLIC INTERFACE $<$<COMPILE_LANGUAGE:CXX>:$<BUILD_INTERFACE:${LIBCXX_SOURCE_DIR}/include>>)

# Enable capturing stack traces for all exceptions.
target_compile_definitions(cxx INTERFACE -DSTD_EXCEPTION_HAS_STACK_TRACE=1)

target_compile_options(cxx INTERFACE $<$<COMPILE_LANGUAGE:CXX>:-nostdinc++>)

# Enable support for Clang-Thread-Safety-Analysis in libcxx
target_compile_definitions(cxx INTERFACE -D_LIBCPP_ENABLE_THREAD_SAFETY_ANNOTATIONS)

#target_link_libraries(cxx PUBLIC cxxabi)

