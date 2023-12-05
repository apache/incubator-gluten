set(VCPKG_BUILD_TYPE release)
set(VCPKG_TARGET_ARCHITECTURE x64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE static)

set(VCPKG_CMAKE_SYSTEM_NAME Linux)

set(VCPKG_C_FLAGS "-mavx2 -mfma -mavx -mf16c -mlzcnt -mbmi2")
set(VCPKG_CXX_FLAGS "-mavx2 -mfma -mavx -mf16c -mlzcnt -std=c++17 -mbmi2")
set(VCPKG_LINKER_FLAGS "-static-libstdc++ -static-libgcc")
