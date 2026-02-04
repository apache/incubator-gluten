set(VCPKG_TARGET_ARCHITECTURE arm64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE static)

set(VCPKG_CMAKE_SYSTEM_NAME Darwin)
set(VCPKG_OSX_ARCHITECTURES arm64)

set(VCPKG_BUILD_TYPE release)


set(VCPKG_C_FLAGS "")
set(VCPKG_CXX_FLAGS "-std=c++20")

if("${PORT}" STREQUAL "grpc")
    set(VCPKG_CXX_FLAGS "${VCPKG_CXX_FLAGS} -Wno-missing-template-arg-list-after-template-kw")
endif()
