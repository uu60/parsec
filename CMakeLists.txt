 cmake_minimum_required(VERSION 3.22)

 project(parsec)

 set(CMAKE_CXX_STANDARD 17)

# Include memory check configuration
include(CMakeMemoryCheck.cmake)

# Set base optimization flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2")


find_package(MPI REQUIRED)
find_package(OpenSSL REQUIRED)

# Try to find TBB, but make it optional
if (APPLE)
    find_package(tbb CONFIG QUIET)
    if (tbb_FOUND)
        set(TBB_FOUND TRUE)
        set(TBB_LIBRARIES TBB::tbb)
    endif()
else ()
    find_package(TBB CONFIG QUIET)
    if (TBB_FOUND)
        set(TBB_LIBRARIES TBB::tbb)
    endif()
endif ()

find_package(Boost REQUIRED CONFIG COMPONENTS program_options)

# Define TBB availability for the code
if (TBB_FOUND)
    message(STATUS "TBB found - TBB thread pool will be available")
    add_compile_definitions(PARSEC_HAS_TBB)
else()
    message(STATUS "TBB not found - TBB thread pool will be disabled")
endif()

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
else()
    target_link_libraries(
            ${PROJECT_NAME} PUBLIC MPI::MPI_CXX OpenSSL::SSL OpenSSL::Crypto Boost::boost Boost::program_options
    )
endif()

add_subdirectory(primitives)
add_subdirectory(db)
add_subdirectory(db/benchmark)

add_executable(test_multiply_comparison test_multiply_comparison.cpp)
target_link_libraries(test_multiply_comparison parsec ${MPI_LIBRARIES} ${OPENSSL_LIBRARIES})

 #
 # # install libxx.so to ${CMAKE_INSTALL_PREFIX}/lib
 # install(
 #         TARGETS ${PROJECT_NAME}
 #         EXPORT ${PROJECT_NAME}Targets
 #         ARCHIVE DESTINATION lib
 #         LIBRARY DESTINATION lib
 #         RUNTIME DESTINATION bin
 # )
 #
 # # headers path
 # install(DIRECTORY include/
 #         DESTINATION include/${PROJECT_NAME}
 #         FILES_MATCHING PATTERN "*.h")
 #
 # # generate xxTargets.cmake
 # install(
 #         EXPORT ${PROJECT_NAME}Targets
 #         FILE ${PROJECT_NAME}Targets.cmake
 #         DESTINATION lib/cmake/${PROJECT_NAME}
 # )
 #
 # # parameters used in xxConfig.cmake
 # set(INCLUDE_DIRS include)
 # set(LIBRARIES ${PROJECT_NAME})
 # set(LIB_DIR lib)
 #
 # # provided by cmake
 # include(CMakePackageConfigHelpers)
 #
 # # generate xxConfigVersion.cmake
 # write_basic_package_version_file(
 #         ${PROJECT_BINARY_DIR}/${PROJECT_NAME}ConfigVersion.cmake
 #         VERSION 0.0.1
 #         COMPATIBILITY SameMajorVersion
 # )
 #
 # # generate xxConfig.cmake
 # configure_package_config_file(
 #         ${PROJECT_SOURCE_DIR}/${PROJECT_NAME}Config.cmake.in
 #         ${PROJECT_BINARY_DIR}/${PROJECT_NAME}Config.cmake
 #         INSTALL_DESTINATION lib/cmake/${PROJECT_NAME}
 #         PATH_VARS INCLUDE_DIRS LIBRARIES LIB_DIR
 #         INSTALL_PREFIX ${CMAKE_INSTALL_PREFIX}
 # )
 #
 # install(
 #         FILES ${PROJECT_BINARY_DIR}/${PROJECT_NAME}Config.cmake ${PROJECT_BINARY_DIR}/${PROJECT_NAME}ConfigVersion.cmake
 #         DESTINATION lib/cmake/${PROJECT_NAME}
 # )
