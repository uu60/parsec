cmake_minimum_required(VERSION 3.22)

project(parsec VERSION 0.1.0 LANGUAGES CXX)

option(PARSEC_BUILD_DATABASE "Build the secure database demo" ON)
option(PARSEC_BUILD_BENCHMARKS "Build benchmark executables" ON)
option(PARSEC_BUILD_EXPERIMENTS "Build research experiment executables" ON)
option(PARSEC_BUILD_TESTS "Build and register correctness tests" OFF)
option(PARSEC_ENABLE_SANITIZERS "Enable AddressSanitizer and UndefinedBehaviorSanitizer" OFF)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if (PARSEC_ENABLE_SANITIZERS)
    if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
        add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
        add_link_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
    else ()
        message(FATAL_ERROR "PARSEC_ENABLE_SANITIZERS requires GCC or Clang")
    endif ()
endif ()

if (PARSEC_BUILD_TESTS)
    include(CTest)
endif ()


find_package(MPI REQUIRED)
find_package(OpenSSL REQUIRED)

 # Try to find TBB, but make it optional
 if (APPLE)
     find_package(tbb CONFIG QUIET)
     if (tbb_FOUND)
         set(TBB_FOUND TRUE)
         set(TBB_LIBRARIES TBB::tbb)
     endif ()
 else ()
     find_package(TBB CONFIG QUIET)
     if (TBB_FOUND)
         set(TBB_LIBRARIES TBB::tbb)
     endif ()
 endif ()

 find_package(Boost REQUIRED CONFIG)

 # Define TBB availability for the code
 if (TBB_FOUND)
     message(STATUS "TBB found - TBB thread pool will be available")
     add_compile_definitions(PARSEC_HAS_TBB)
 else ()
     message(STATUS "TBB not found - TBB thread pool will be disabled")
 endif ()

add_subdirectory(runtime)
add_subdirectory(primitives)

if (PARSEC_BUILD_DATABASE)
    add_subdirectory(db)
    if (PARSEC_BUILD_BENCHMARKS)
        add_subdirectory(db/benchmark)
    endif ()
endif ()
