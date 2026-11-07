# Minimal project that uses an installed Parsec through find_package.
#
#   cmake --install build/release --prefix /opt/parsecdb
#   cmake -S examples/find-package -B build/find-package -DCMAKE_PREFIX_PATH=/opt/parsecdb
#   cmake --build build/find-package
cmake_minimum_required(VERSION 3.22)
project(parsecdb_find_package_example LANGUAGES CXX)

find_package(parsecdb 0.1 REQUIRED)

add_executable(parsecdb_example main.cpp)
target_link_libraries(parsecdb_example PRIVATE parsecdb::db)
