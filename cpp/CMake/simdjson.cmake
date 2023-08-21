include_guard(GLOBAL)
include(FetchContent)
function(set_with_default var_name envvar_name default)
  if(DEFINED ENV{${envvar_name}})
    set(${var_name}
        $ENV{${envvar_name}}
        PARENT_SCOPE)
  else()
    set(${var_name}
        ${default}
        PARENT_SCOPE)
  endif()
endfunction()

macro(resolve_dependency_url dependency_name)
  # Prepend prefix for default checksum.
  string(PREPEND VELOX_${dependency_name}_BUILD_SHA256_CHECKSUM "SHA256=")

  set_with_default(
    VELOX_${dependency_name}_SOURCE_URL VELOX_${dependency_name}_URL
    ${VELOX_${dependency_name}_SOURCE_URL})
  if(DEFINED ENV{VELOX_${dependency_name}_URL})
    set_with_default(VELOX_${dependency_name}_BUILD_SHA256_CHECKSUM
                     VELOX_${dependency_name}_SHA256 "")
    if(DEFINED ENV{VELOX_${dependency_name}_SHA256})
      string(PREPEND VELOX_${dependency_name}_BUILD_SHA256_CHECKSUM "SHA256=")
    endif()
  endif()
endmacro()

set(VELOX_SIMDJSON_VERSION 3.1.5)
set(VELOX_SIMDJSON_BUILD_SHA256_CHECKSUM
    5b916be17343324426fc467a4041a30151e481700d60790acfd89716ecc37076)
set(VELOX_SIMDJSON_SOURCE_URL
    "https://github.com/simdjson/simdjson/archive/refs/tags/v${VELOX_SIMDJSON_VERSION}.tar.gz"
)

resolve_dependency_url(SIMDJSON)

message(STATUS "Building simdjson from source")

FetchContent_Declare(
  simdjson
  URL ${VELOX_SIMDJSON_SOURCE_URL}
  URL_HASH ${VELOX_SIMDJSON_BUILD_SHA256_CHECKSUM})

FetchContent_MakeAvailable(simdjson)

