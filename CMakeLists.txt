cmake_minimum_required(VERSION 3.22)

project(parsec VERSION 0.1.0 LANGUAGES CXX)

option(PARSEC_BUILD_DATABASE "Build the secure database components" ON)
option(PARSEC_BUILD_BENCHMARKS "Build benchmark executables" ON)
option(PARSEC_BUILD_EXPERIMENTS "Build research experiment executables" ON)
option(PARSEC_BUILD_TESTS "Build and register correctness tests" OFF)
option(PARSEC_ENABLE_SANITIZERS "Enable AddressSanitizer and UndefinedBehaviorSanitizer" OFF)
option(PARSEC_ENABLE_WARNINGS "Compile Parsec sources with -Wall -Wextra" ON)
option(PARSEC_WARNINGS_AS_ERRORS "Treat compiler warnings as errors" OFF)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Warning flags apply to this project's own targets only; they are not propagated to projects that consume Parsec.
if (PARSEC_ENABLE_WARNINGS AND CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    add_compile_options(-Wall -Wextra)
    if (PARSEC_WARNINGS_AS_ERRORS)
        # Existing signed/unsigned comparisons and unused parameters are known debt and stay warnings; every other
        # warning fails the build.
        add_compile_options(-Werror -Wno-error=sign-compare -Wno-error=unused-parameter)
    endif ()
endif ()

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
    # `mpiexec --version` alone is not reliable: the Debian/Ubuntu Open MPI 5 packages (Ubuntu 26.04) do not install
    # PRRTE's help files, so `--version` prints only a "couldn't open the help file" error. Also look at the MPI
    # library version string and at the resolved launcher path (e.g. /usr/bin/mpirun.openmpi).
    execute_process(COMMAND ${MPIEXEC_EXECUTABLE} --version
            OUTPUT_VARIABLE PARSEC_MPIEXEC_VERSION ERROR_VARIABLE PARSEC_MPIEXEC_VERSION
            RESULT_VARIABLE PARSEC_MPIEXEC_VERSION_RESULT)
    get_filename_component(PARSEC_MPIEXEC_REALPATH "${MPIEXEC_EXECUTABLE}" REALPATH)
    set(PARSEC_MPI_ID "${PARSEC_MPIEXEC_VERSION};${PARSEC_MPIEXEC_REALPATH};${MPI_CXX_LIBRARY_VERSION_STRING}")
    if (PARSEC_MPI_ID MATCHES "Open MPI|OpenRTE|PRRTE|prterun|openmpi|schizo-ompi")
        set(PARSEC_MPIEXEC_TEST_FLAGS "--oversubscribe")
    endif ()
endif ()
# Environment for MPI test runs. Open MPI's memory patcher rewrites malloc/munmap hooks, which crashes ASan builds
# inside MPI_Init on Open MPI 5 (UCX component). The setting is ignored by other MPI implementations.
set(PARSEC_MPI_TEST_ENVIRONMENT "")
if (PARSEC_ENABLE_SANITIZERS)
    list(APPEND PARSEC_MPI_TEST_ENVIRONMENT "OMPI_MCA_memory=^patcher")
endif ()
if (PARSEC_BUILD_TESTS)
    message(STATUS "MPI test launcher: ${MPIEXEC_EXECUTABLE} ${PARSEC_MPIEXEC_TEST_FLAGS}")
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

include(GNUInstallDirs)
# Installed headers keep their in-tree layout (for example `utils/Math.h`) under include/parsec.
set(PARSEC_INSTALL_INCLUDEDIR "${CMAKE_INSTALL_INCLUDEDIR}/parsec")

# Wrap source-tree include directories so they are used while building but not exported to consumers.
function(parsec_build_include_dirs out)
    set(result "")
    foreach (dir ${ARGN})
        list(APPEND result "$<BUILD_INTERFACE:${dir}>")
    endforeach ()
    set(${out} ${result} PARENT_SCOPE)
endfunction()

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

# Package export: `find_package(parsec)` provides parsec::parsec and, when built, parsec::db.
include(CMakePackageConfigHelpers)
set(PARSEC_INSTALL_CMAKEDIR "${CMAKE_INSTALL_LIBDIR}/cmake/parsec")
install(EXPORT parsecTargets
        NAMESPACE parsec::
        DESTINATION ${PARSEC_INSTALL_CMAKEDIR})
configure_package_config_file(
        "${CMAKE_CURRENT_SOURCE_DIR}/cmake/parsecConfig.cmake.in"
        "${CMAKE_CURRENT_BINARY_DIR}/parsecConfig.cmake"
        INSTALL_DESTINATION ${PARSEC_INSTALL_CMAKEDIR})
write_basic_package_version_file(
        "${CMAKE_CURRENT_BINARY_DIR}/parsecConfigVersion.cmake"
        COMPATIBILITY SameMinorVersion)
install(FILES
        "${CMAKE_CURRENT_BINARY_DIR}/parsecConfig.cmake"
        "${CMAKE_CURRENT_BINARY_DIR}/parsecConfigVersion.cmake"
        DESTINATION ${PARSEC_INSTALL_CMAKEDIR})
