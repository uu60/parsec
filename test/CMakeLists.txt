cmake_minimum_required(VERSION 3.22)
set(CMAKE_C_COMPILER mpicc)
set(CMAKE_CXX_COMPILER mpicxx)

set(CMAKE_CXX_FLAGS " ${CMAKE_CXX_FLAGS} -g -fsanitize=address")

project(demo)

set(CMAKE_CXX_STANDARD 17)

find_package(secure_2pc_package REQUIRED)

add_executable(demo main.cpp
        test_cases.h)

target_link_libraries(demo secure_2pc_package)
target_link_directories(demo PUBLIC ${secure_2pc_package_LIBRARY_DIRS})
target_include_directories(demo PUBLIC ${secure_2pc_package_INCLUDE_DIRS})
