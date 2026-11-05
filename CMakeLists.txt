cmake_minimum_required(VERSION 3.22)

project(parsec VERSION 0.1.0 LANGUAGES CXX)

option(PARSEC_BUILD_DATABASE "Build the secure database demo" ON)
option(PARSEC_BUILD_SQL_CLI "Build the SQL command-line demonstration" "${PARSEC_BUILD_DATABASE}")
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

# Tests always start three ranks. Open MPI refuses to start more ranks than detected slots (for example on two-core CI
# runners) unless oversubscription is allowed.
set(PARSEC_MPIEXEC_TEST_FLAGS "" CACHE STRING "Extra mpiexec flags used when running MPI tests")
if (PARSEC_BUILD_TESTS AND PARSEC_MPIEXEC_TEST_FLAGS STREQUAL "")
    execute_process(COMMAND ${MPIEXEC_EXECUTABLE} --version
            OUTPUT_VARIABLE PARSEC_MPIEXEC_VERSION ERROR_VARIABLE PARSEC_MPIEXEC_VERSION
            RESULT_VARIABLE PARSEC_MPIEXEC_VERSION_RESULT)
    if (PARSEC_MPIEXEC_VERSION MATCHES "Open MPI|OpenRTE|PRRTE|prterun")
        set(PARSEC_MPIEXEC_TEST_FLAGS "--oversubscribe")
    endif ()
endif ()
separate_arguments(PARSEC_MPIEXEC_TEST_FLAGS_LIST NATIVE_COMMAND "${PARSEC_MPIEXEC_TEST_FLAGS}")
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
        add_subdirectory(benchmarks/database)
    endif ()
    if (PARSEC_BUILD_EXPERIMENTS)
        add_subdirectory(experiments/database)
    endif ()
endif ()

if (PARSEC_BUILD_SQL_CLI)
    if (NOT PARSEC_BUILD_DATABASE)
        message(FATAL_ERROR "PARSEC_BUILD_SQL_CLI requires PARSEC_BUILD_DATABASE")
    endif ()
    add_subdirectory(apps/sql_cli)
endif ()
