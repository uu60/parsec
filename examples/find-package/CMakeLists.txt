# Minimal project that uses an installed Parsec through find_package.
#
#   cmake --install build/release --prefix /opt/parsec
#   cmake -S examples/find-package -B build/find-package -DCMAKE_PREFIX_PATH=/opt/parsec
#   cmake --build build/find-package
cmake_minimum_required(VERSION 3.22)
project(parsec_find_package_example LANGUAGES CXX)

find_package(parsec 0.1 REQUIRED)

add_executable(parsec_example main.cpp)
target_link_libraries(parsec_example PRIVATE parsec::db)
