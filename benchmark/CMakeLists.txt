cmake_minimum_required(VERSION 3.27)

#set(CMAKE_CXX_FLAGS " ${CMAKE_CXX_FLAGS} -g -fsanitize=address")
set(CMAKE_CXX_FLAGS " ${CMAKE_CXX_FLAGS} -O2")

project(demo)

set(CMAKE_CXX_STANDARD 17)

find_package(secure_2pc_package REQUIRED)
find_package(MPI REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(tbb REQUIRED)
find_package(Boost REQUIRED)

add_executable(demo main.cpp
        test_cases.h)

target_link_libraries(demo secure_2pc_package)
target_link_directories(demo PUBLIC ${secure_2pc_package_LIBRARY_DIRS})
target_include_directories(demo PUBLIC ${secure_2pc_package_INCLUDE_DIRS} ${Boost_INCLUDE_DIRS})
