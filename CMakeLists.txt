 cmake_minimum_required(VERSION 3.22)

 project(parsec)

 set(CMAKE_CXX_STANDARD 17)

 # Set base optimization flags
 set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")

 set(PARSEC_SIMD_TARGET "native" CACHE STRING "SIMD target: portable, native, or avx512")
 set_property(CACHE PARSEC_SIMD_TARGET PROPERTY STRINGS portable native avx512)
 if (PARSEC_SIMD_TARGET STREQUAL "native")
     add_compile_options(-march=native)
 elseif (PARSEC_SIMD_TARGET STREQUAL "avx512")
     if (NOT CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64|AMD64|amd64")
         message(FATAL_ERROR "PARSEC_SIMD_TARGET=avx512 requires an x86-64 target")
     endif ()
     add_compile_options(-mavx512f -mavx512dq -mavx512bw -mavx512vl)
 elseif (NOT PARSEC_SIMD_TARGET STREQUAL "portable")
     message(FATAL_ERROR "Unknown PARSEC_SIMD_TARGET=${PARSEC_SIMD_TARGET}")
 endif ()
 message(STATUS "Parsec SIMD target: ${PARSEC_SIMD_TARGET}")


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
 add_subdirectory(db)
 add_subdirectory(db/benchmark)
