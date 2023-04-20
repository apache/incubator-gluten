
cmake_minimum_required(VERSION 3.20)
set(CH_SOURCE_DIR ${CMAKE_SOURCE_DIR}/ClickHouse CACHE STRING "ClickHouse source dir")

project(libch LANGUAGES C CXX ASM)
file(GLOB clickhouse_files "${CH_SOURCE_DIR}/*")
if ("${CH_SOURCE_DIR}" STREQUAL "${CMAKE_SOURCE_DIR}/ClickHouse")
    if ("${CH_SOURCE_DIR}" STREQUAL "${CMAKE_SOURCE_DIR}/ClickHouse")
        if (NOT clickhouse_files)
            execute_process(COMMAND git clone --recursive --depth 1 https://github.com/Kyligence/ClickHouse.git ${CH_SOURCE_DIR}
                    OUTPUT_VARIABLE download_ch)
            execute_process(COMMAND cd ${CH_SOURCE_DIR} && git pull && git submodule update --init --recursive --force --depth 1
                    OUTPUT_VARIABLE download_ch)
        endif()
    endif()
else()
    if (NOT clickhouse_files)
        # Checking out *all* submodules takes > 5 min. Therefore, the smoke build ("FastTest") in CI initializes only the set of
        # submodules minimally needed for a build and we cannot assume here that all submodules are populated.
        message(ERROR "clickhouse ${CH_SOURCE_DIR} is missing or empty. to fix try run:")
        message(STATUS "    git clone --recursive --depth 1 https://github.com/Kyligence/ClickHouse.git ${CMAKE_SOURCE_DIR}")
    endif()
endif()

if (NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/local-engine/proto/substrait")
    execute_process(COMMAND ln -s ${CMAKE_CURRENT_SOURCE_DIR}/../gluten-core/src/main/resources/substrait/proto/substrait ${CMAKE_CURRENT_SOURCE_DIR}/local-engine/proto/substrait)
endif ()

if (NOT EXISTS "${CH_SOURCE_DIR}/utils/extern-local-engine/")
    execute_process(COMMAND ln -s ${CMAKE_CURRENT_SOURCE_DIR}/local-engine ${CH_SOURCE_DIR}/utils/extern-local-engine)
endif ()

set(CH_BINARY_DIR "${CMAKE_CURRENT_SOURCE_DIR}/build")

add_custom_command(
        USES_TERMINAL
        COMMAND
        bash -c
        \"cmake -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        -DENABLE_PROTOBUF=ON
        -DENABLE_TESTS=OFF
        -DENABLE_JEMALLOC=ON
        -DENABLE_MULTITARGET_CODE=ON
        -DENABLE_EXTERN_LOCAL_ENGINE=ON
        -S ${CH_SOURCE_DIR} -G Ninja -B ${CH_BINARY_DIR} &&
        cmake --build ${CH_BINARY_DIR} --target ch\"
        OUTPUT _build_ch
)

add_custom_target(build_ch ALL DEPENDS _build_ch)
