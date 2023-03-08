add_library(global-group INTERFACE)
add_library(global-libs INTERFACE)
add_subdirectory(cmake/cxxabi)
add_subdirectory(cmake/cxx)
target_link_libraries(global-libs INTERFACE cxx cxxabi)

set (DEFAULT_LIBS "-nodefaultlibs")
execute_process (COMMAND ${CMAKE_CXX_COMPILER} --target=${CMAKE_CXX_COMPILER_TARGET} --print-libgcc-file-name --rtlib=compiler-rt OUTPUT_VARIABLE BUILTINS_LIBRARY OUTPUT_STRIP_TRAILING_WHITESPACE)
if (NOT EXISTS "${BUILTINS_LIBRARY}")
    set (BUILTINS_LIBRARY "-lgcc")
endif ()
set (DEFAULT_LIBS "${DEFAULT_LIBS} ${BUILTINS_LIBRARY} ${COVERAGE_OPTION} -lc -lm -lrt -lpthread -ldl")
set(CMAKE_CXX_STANDARD_LIBRARIES ${DEFAULT_LIBS})
set(CMAKE_C_STANDARD_LIBRARIES ${DEFAULT_LIBS})

# Unfortunately '-pthread' doesn't work with '-nodefaultlibs'.
# Just make sure we have pthreads at all.
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
link_libraries(global-group)
target_link_libraries(global-group INTERFACE
        -Wl,--start-group
        $<TARGET_PROPERTY:global-libs,INTERFACE_LINK_LIBRARIES>
        -Wl,--end-group
        )