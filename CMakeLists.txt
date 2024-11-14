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
        src/int/multiplication/MulExecutor.cpp
        include/int/multiplication/MulExecutor.h
        src/int/addition/AddExecutor.cpp
        include/int/addition/AddExecutor.h
        src/ot/RsaOtExecutor.cpp
        include/ot/RsaOtExecutor.h
        src/utils/Comm.cpp
        include/utils/Comm.h
        src/utils/Crypto.cpp
        include/utils/Crypto.h
        src/utils/System.cpp
        include/utils/System.h
        src/bit/xor/BitXorExecutor.cpp
        include/bit/xor/BitXorExecutor.h
        src/bit/and/BitAndExecutor.cpp
        include/bit/and/BitAndExecutor.h
        src/bmt/OtBmtGenerator.cpp
        include/bmt/OtBmtGenerator.h
        src/bmt/AbstractTripleGenerator.cpp
        include/bmt/AbstractTripleGenerator.h
        src/bmt/FixedTripleGenerator.cpp
        include/bmt/FixedTripleGenerator.h
        src/int/comparison/ArithLessThanExecutor.cpp
        include/int/comparison/ArithLessThanExecutor.h
        src/int/IntArithExecutor.cpp
        include/int/IntArithExecutor.h
        src/bit/BitExecutor.cpp
        include/bit/BitExecutor.h
        src/api/Secret.cpp
        include/api/Secret.h
        src/api/IntSecret.cpp
        include/api/IntSecret.h
        src/api/BitSecret.cpp
        include/api/BitSecret.h
        src/int/comparison/MuxExecutor.cpp
        include/int/comparison/MuxExecutor.h
        include/int/IntBoolExecutor.h
        src/int/IntBoolExecutor.cpp
        include/int/and/IntAndExecutor.h
        include/int/and/IntAndExecutor.cpp
)
if (EXISTS "include/bmt/fixed_triples_0.h")
    add_library(${PROJECT_NAME} SHARED
            include/bmt/fixed_triples_0.h
            include/int/fixed_ab_pairs_0.h
    )
endif ()
if (EXISTS "include/bmt/fixed_triples_1.h")
    add_library(${PROJECT_NAME} SHARED
            include/bmt/fixed_triples_1.h
            include/int/fixed_ab_pairs_1.h
    )
endif ()
target_include_directories(${PROJECT_NAME} PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>)
#set_target_properties(${PROJECT_NAME} PROPERTIES PUBLIC_HEADER "include/executor/int/AbstractAdditionShareExecutor.h;include/executor/Executor.h")

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
