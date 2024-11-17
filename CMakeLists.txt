cmake_minimum_required(VERSION 3.22)
project(mpc_package)

set(CMAKE_CXX_STANDARD 17)

find_package(MPI REQUIRED)
find_package(OpenSSL REQUIRED)

add_library(${PROJECT_NAME} SHARED
        src/utils/Math.cpp
        include/utils/Math.h
        src/SecureExecutor.cpp
        include/SecureExecutor.h
        src/utils/Log.cpp
        include/utils/Log.h
        src/compute/arith/MulExecutor.cpp
        include/compute/arith/MulExecutor.h
        src/compute/arith/AddExecutor.cpp
        include/compute/arith/AddExecutor.h
        src/ot/RsaOtExecutor.cpp
        include/ot/RsaOtExecutor.h
        src/utils/Comm.cpp
        include/utils/Comm.h
        src/utils/Crypto.cpp
        include/utils/Crypto.h
        src/utils/System.cpp
        include/utils/System.h
        src/bmt/OtBmtGenerator.cpp
        include/bmt/OtBmtGenerator.h
        src/compute/arith/LessThanArithExecutor.cpp
        include/compute/arith/LessThanArithExecutor.h
        src/compute/ArithExecutor.cpp
        include/compute/ArithExecutor.h
        src/api/IntSecret.cpp
        include/api/IntSecret.h
        src/compute/arith/MuxArithExecutor.cpp
        include/compute/arith/MuxArithExecutor.h
        include/compute/BoolExecutor.h
        src/compute/BoolExecutor.cpp
        include/compute/bool/BitwiseAndExecutor.h
        src/compute/bool/BitwiseAndExecutor.cpp
        include/compute/bool/BitwiseXorExecutor.h
        src/compute/bool/BitwiseXorExecutor.cpp
        include/compute/bool/EqualBoolExecutor.h
        src/compute/bool/EqualBoolExecutor.cpp
        src/api/BitSecret.cpp
        include/bmt/BMT.h
)
if (EXISTS "include/bmt/fixed_triples_0.h")
    add_library(${PROJECT_NAME} SHARED
            include/bmt/fixed_triples_0.h
            include/compute/fixed_ab_pairs_0.h
    )
endif ()
if (EXISTS "include/bmt/fixed_triples_1.h")
    add_library(${PROJECT_NAME} SHARED
            include/bmt/fixed_triples_1.h
            include/compute/fixed_ab_pairs_1.h
    )
endif ()
target_include_directories(${PROJECT_NAME} PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>)
#set_target_properties(${PROJECT_NAME} PROPERTIES PUBLIC_HEADER "include/executor/compute/AbstractarithShareExecutor.h;include/executor/Executor.h")

# install libxx.so to ${CMAKE_INSTALL_PREFIX}/lib
install(
        TARGETS ${PROJECT_NAME}
        EXPORT ${PROJECT_NAME}Targets
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib
        RUNTIME DESTINATION bin
)
# headers path
install(DIRECTORY include/
        DESTINATION include/${PROJECT_NAME}
        FILES_MATCHING PATTERN "*.h")

# generate xxTargets.cmake
install(
        EXPORT ${PROJECT_NAME}Targets
        FILE ${PROJECT_NAME}Targets.cmake
        DESTINATION lib/cmake/${PROJECT_NAME}
)

# parameters used in xxConfig.cmake
set(INCLUDE_DIRS include)
set(LIBRARIES ${PROJECT_NAME})
set(LIB_DIR lib)

# provided by cmake
include(CMakePackageConfigHelpers)

# generate xxConfigVersion.cmake
write_basic_package_version_file(
        ${PROJECT_BINARY_DIR}/${PROJECT_NAME}ConfigVersion.cmake
        VERSION 0.0.1
        COMPATIBILITY SameMajorVersion
)

# generate xxConfig.cmake
configure_package_config_file(
        ${PROJECT_SOURCE_DIR}/${PROJECT_NAME}Config.cmake.in
        ${PROJECT_BINARY_DIR}/${PROJECT_NAME}Config.cmake
        INSTALL_DESTINATION lib/cmake/${PROJECT_NAME}
        PATH_VARS INCLUDE_DIRS LIBRARIES LIB_DIR
        INSTALL_PREFIX ${CMAKE_INSTALL_PREFIX}
)

install(
        FILES ${PROJECT_BINARY_DIR}/${PROJECT_NAME}Config.cmake ${PROJECT_BINARY_DIR}/${PROJECT_NAME}ConfigVersion.cmake
        DESTINATION lib/cmake/${PROJECT_NAME}
)

target_link_libraries(
        ${PROJECT_NAME} PUBLIC MPI::MPI_CXX OpenSSL::SSL OpenSSL::Crypto
)
