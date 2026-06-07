cmake_minimum_required(VERSION 3.20)

project(tpch_q13_native LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_CONFIGURATION_TYPES AND NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

set(PYARROW_DIR "${CMAKE_SOURCE_DIR}/q13env/Lib/site-packages/pyarrow")
set(PYARROW_INCLUDE_DIR "${PYARROW_DIR}/include")
set(PYARROW_RUNTIME_DIR "${CMAKE_SOURCE_DIR}/q13env/Lib/site-packages/pyarrow.libs")

add_executable(q13_native
    native/main.cpp
    native/q13_utils.cpp
    native/q13_parquet.cpp
    native/q13_processing.cpp
    native/q13_engine.cpp
    native/q13_cli.cpp
)

target_include_directories(q13_native PRIVATE "${PYARROW_INCLUDE_DIR}" native/)
target_link_directories(q13_native PRIVATE "${PYARROW_DIR}")
target_link_libraries(q13_native PRIVATE arrow parquet)

if(MSVC)
    target_compile_options(q13_native PRIVATE /O2 /EHsc /DNOMINMAX)
else()
    target_compile_options(q13_native PRIVATE -O3)
endif()

add_custom_command(TARGET q13_native POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "${PYARROW_DIR}/arrow.dll"
        "$<TARGET_FILE_DIR:q13_native>/arrow.dll"
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "${PYARROW_DIR}/parquet.dll"
        "$<TARGET_FILE_DIR:q13_native>/parquet.dll"
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "${PYARROW_RUNTIME_DIR}/msvcp140-2a7008da6f9aae43c7a434835dcaf31e.dll"
        "$<TARGET_FILE_DIR:q13_native>/msvcp140-2a7008da6f9aae43c7a434835dcaf31e.dll")