 cmake_minimum_required(VERSION 3.22)

 project(parsec)

 set(CMAKE_CXX_STANDARD 17)

 # Set base optimization flags
 set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")


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

 find_package(Boost REQUIRED CONFIG COMPONENTS program_options)

 # Define TBB availability for the code
 if (TBB_FOUND)
     message(STATUS "TBB found - TBB thread pool will be available")
     add_compile_definitions(PARSEC_HAS_TBB)
 else ()
     message(STATUS "TBB not found - TBB thread pool will be disabled")
 endif ()

 file(GLOB_RECURSE LIB_SOURCES
         "primitives/include/*.h"
         "primitives/include/*.cpp"
         "primitives/src/*.cpp"
 )

 add_library(${PROJECT_NAME} STATIC ${LIB_SOURCES})

 target_include_directories(${PROJECT_NAME} PUBLIC
         $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/primitives/include>
         $<INSTALL_INTERFACE:include>)

 # Link libraries conditionally based on TBB availability
 if (TBB_FOUND)
     target_link_libraries(
             ${PROJECT_NAME} PUBLIC MPI::MPI_CXX OpenSSL::SSL OpenSSL::Crypto ${TBB_LIBRARIES} Boost::boost Boost::program_options
     )
 else ()
     target_link_libraries(
             ${PROJECT_NAME} PUBLIC MPI::MPI_CXX OpenSSL::SSL OpenSSL::Crypto Boost::boost Boost::program_options
     )
 endif ()

 add_subdirectory(primitives)
 add_subdirectory(db)
 add_subdirectory(db/benchmark)
