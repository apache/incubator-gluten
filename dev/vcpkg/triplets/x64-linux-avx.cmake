set(VCPKG_TARGET_ARCHITECTURE x64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE static)

set(VCPKG_CMAKE_SYSTEM_NAME Linux)

set(VCPKG_C_FLAGS "-mavx2 -mfma -mavx -mf16c -mlzcnt -mbmi2")
set(VCPKG_CXX_FLAGS "-mavx2 -mfma -mavx -mf16c -mlzcnt -std=c++17 -mbmi2")
set(VCPKG_LINKER_FLAGS "-static-libstdc++ -static-libgcc")

# Fix folly static link libstdc++
# See: https://github.com/facebook/folly/blob/b88123c2abf4b3244ed285e6db0d4bea2d24f95f/CMakeLists.txt#L192
if(PORT MATCHES "folly")
    set(VCPKG_CMAKE_CONFIGURE_OPTIONS "-DFOLLY_NO_EXCEPTION_TRACER=ON")
endif()

# Avoid build both release and debug
set(VCPKG_BUILD_TYPE "release")
