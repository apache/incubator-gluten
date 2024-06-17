include_guard(GLOBAL)
include(FetchContent)

set(GLUTEN_GTEST_VERSION 1.13.0)
set(GLUTEN_GTEST_BUILD_SHA256_CHECKSUM
    ad7fdba11ea011c1d925b3289cf4af2c66a352e18d4c7264392fead75e919363)
set(GLUTEN_GTEST_SOURCE_URL
    "https://github.com/google/googletest/archive/refs/tags/v${GLUTEN_GTEST_VERSION}.tar.gz"
)

resolve_dependency_url(GTEST)

message(STATUS "Building gtest from source")
FetchContent_Declare(
  gtest
  URL ${GLUTEN_GTEST_SOURCE_URL}
  URL_HASH "${GLUTEN_GTEST_BUILD_SHA256_CHECKSUM}")

FetchContent_MakeAvailable(gtest)
