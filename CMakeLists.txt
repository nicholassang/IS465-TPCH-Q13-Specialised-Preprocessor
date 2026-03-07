cmake_minimum_required(VERSION 3.20)
project(tpch_q3)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Arrow include & lib paths from Conda
set(ARROW_INCLUDE_DIR "C:/Users/nicho/miniconda3/envs/myenv/Library/include")
set(ARROW_LIB_DIR     "C:/Users/nicho/miniconda3/envs/myenv/Library/lib")

include_directories(${ARROW_INCLUDE_DIR})
include_directories(${CMAKE_SOURCE_DIR}/include)
link_directories(${ARROW_LIB_DIR})

# Source files
set(SOURCES
    src/main.cpp
    src/scan_parquet.cpp
    src/hash_join.cpp
    src/aggregation.cpp
    src/topk.cpp
)

add_executable(tpch_q3 ${SOURCES})

# Link Arrow libraries
target_link_libraries(tpch_q3
    arrow
    parquet
)