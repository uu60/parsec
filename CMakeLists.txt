 cmake_minimum_required(VERSION 3.22)

 project(secure_2pc_package)

 set(CMAKE_CXX_STANDARD 17)
 set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2")
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -fsanitize=address")


 find_package(MPI REQUIRED)
 find_package(OpenSSL REQUIRED)
 if(APPLE)
  find_package(tbb CONFIG REQUIRED)
 else()
  find_package(TBB CONFIG REQUIRED)
 endif()
 find_package(Boost REQUIRED CONFIG)


 file(GLOB_RECURSE LIB_SOURCES
         "include/*.h"
         "include/*.cpp"
         "src/*.cpp"
 )

 add_library(${PROJECT_NAME} STATIC ${LIB_SOURCES})

 target_include_directories(${PROJECT_NAME} PUBLIC
         $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<INSTALL_INTERFACE:include>)

 target_link_libraries(
         ${PROJECT_NAME} PUBLIC MPI::MPI_CXX OpenSSL::SSL OpenSSL::Crypto TBB::tbb Boost::boost
 )

 file(GLOB CPP_FILES "benchmark/*.cpp")

 foreach(source_file ${CPP_FILES})
  get_filename_component(exe_name ${source_file} NAME_WE)
  add_executable(${exe_name} ${source_file})
  target_link_libraries(${exe_name} PRIVATE ${PROJECT_NAME})
  target_include_directories(${exe_name} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
 endforeach()
# file(GLOB_RECURSE BENCHMARK_SOURCES
#         "benchmark/*.h"
#         "benchmark/*.cpp"
# )
#
# set(BENCHMARK_RUNNER benchmark_runner)
#
# add_executable(${BENCHMARK_RUNNER} ${BENCHMARK_SOURCES})
#
# target_link_libraries(${BENCHMARK_RUNNER} PRIVATE ${PROJECT_NAME})
#
# target_include_directories(${BENCHMARK_RUNNER} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)


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