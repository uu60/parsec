cmake_minimum_required(VERSION 3.22)

project(Dekopon_SS_SMPC_package)

set(CMAKE_CXX_STANDARD 17)

find_package(MPI REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(folly REQUIRED)
find_package(gflags REQUIRED)

add_library(${PROJECT_NAME} SHARED
        src/utils/Math.cpp
        include/utils/Math.h
        src/AbstractSecureExecutor.cpp
        include/AbstractSecureExecutor.h
        src/compute/arith/ArithMulExecutor.cpp
        include/compute/arith/ArithMulExecutor.h
        src/compute/arith/ArithAddExecutor.cpp
        include/compute/arith/ArithAddExecutor.h
        src/ot/RsaOtExecutor.cpp
        include/ot/RsaOtExecutor.h
        src/comm/MpiComm.cpp
        include/comm/MpiComm.h
        src/utils/Crypto.cpp
        include/utils/Crypto.h
        src/utils/System.cpp
        include/utils/System.h
        src/intermediate/OtBmtGenerator.cpp
        include/intermediate/OtBmtGenerator.h
        src/compute/arith/ArithLessThanExecutor.cpp
        include/compute/arith/ArithLessThanExecutor.h
        src/compute/ArithExecutor.cpp
        include/compute/ArithExecutor.h
        src/api/IntSecret.cpp
        include/api/IntSecret.h
        src/compute/arith/ArithMuxExecutor.cpp
        include/compute/arith/ArithMuxExecutor.h
        include/compute/BoolExecutor.h
        src/compute/BoolExecutor.cpp
        include/compute/bool/BoolAndExecutor.h
        src/compute/bool/BoolAndExecutor.cpp
        include/compute/bool/BoolXorExecutor.h
        src/compute/bool/BoolXorExecutor.cpp
        include/compute/bool/BoolEqualExecutor.h
        src/compute/bool/BoolEqualExecutor.cpp
        src/api/BitSecret.cpp
        include/intermediate/BMT.h
        include/comm/IComm.h
        include/intermediate/IntermediateDataSupport.h
        src/intermediate/IntermediateDataSupport.cpp
        src/compute/arith/ArithToBoolExecutor.cpp
        include/compute/arith/ArithToBoolExecutor.h
        src/compute/bool/BoolToArithExecutor.cpp
        include/compute/bool/BoolToArithExecutor.h
        include/intermediate/ABPair.h
        src/intermediate/ABPairGenerator.cpp
        include/intermediate/ABPairGenerator.h
        src/comm/IComm.cpp
)

target_include_directories(${PROJECT_NAME} PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>)

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
        ${PROJECT_NAME} PUBLIC MPI::MPI_CXX OpenSSL::SSL OpenSSL::Crypto Folly::folly gflags
)
