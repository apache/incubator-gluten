set(VCPKG_MANIFEST_DIR $ENV{VCPKG_MANIFEST_DIR})
set(VCPKG_TARGET_TRIPLET $ENV{VCPKG_TRIPLET})
set(VCPKG_HOST_TRIPLET $ENV{VCPKG_TRIPLET})
set(VCPKG_INSTALLED_DIR $ENV{VCPKG_MANIFEST_DIR}/vcpkg_installed)

# Force read CMAKE_PREFIX_PATH from env
set(CMAKE_PREFIX_PATH $ENV{CMAKE_PREFIX_PATH})

include($ENV{VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake)

# set(CMAKE_EXE_LINKER_FLAGS "-static-libstdc++ -static-libgcc")
# set(CMAKE_SHARED_LINKER_FLAGS "-static-libstdc++ -static-libgcc")