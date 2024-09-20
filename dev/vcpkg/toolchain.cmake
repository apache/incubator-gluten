# This file will be used by cmake before cmake function `project(xxx)`
# is executed, even though it's an external cmake project.

set(ENABLE_GLUTEN_VCPKG ON)

# If this arg is set, `vcpkg install` will be executed according
# to the manifest file exists in this given path, i.e., vcpkg.json,
# which will not respect our setting for extra features through
# `--x-feature`.
#set(VCPKG_MANIFEST_DIR $ENV{VCPKG_MANIFEST_DIR})

set(VCPKG_TARGET_TRIPLET $ENV{VCPKG_TRIPLET})
set(VCPKG_HOST_TRIPLET $ENV{VCPKG_TRIPLET})
set(VCPKG_INSTALLED_DIR $ENV{VCPKG_MANIFEST_DIR}/vcpkg_installed)
set(VCPKG_INSTALL_OPTIONS --no-print-usage)

# Force read CMAKE_PREFIX_PATH from env
set(CMAKE_PREFIX_PATH $ENV{CMAKE_PREFIX_PATH})

include($ENV{VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake)

set(CMAKE_EXE_LINKER_FLAGS "-static-libstdc++ -static-libgcc")
set(CMAKE_SHARED_LINKER_FLAGS "-static-libstdc++ -static-libgcc")

# Disable boost new version warning for FindBoost module
set(Boost_NO_WARN_NEW_VERSIONS ON)
