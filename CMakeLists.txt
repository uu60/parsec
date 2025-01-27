cmake_minimum_required(VERSION 3.22)

project(secure_2pc_package)

set(CMAKE_CXX_STANDARD 17)

find_package(MPI REQUIRED)
find_package(OpenSSL REQUIRED)

add_library(${PROJECT_NAME} SHARED
        src/utils/Math.cpp
        include/utils/Math.h
        src/AbstractSecureExecutor.cpp
        include/AbstractSecureExecutor.h
        src/compute/arith/ArithMultiplyExecutor.cpp
        include/compute/arith/ArithMultiplyExecutor.h
        src/compute/arith/ArithAddExecutor.cpp
        include/compute/arith/ArithAddExecutor.h
        src/ot/BaseOtExecutor.cpp
        include/ot/BaseOtExecutor.h
        src/comm/MpiComm.cpp
        include/comm/MpiComm.h
        src/utils/Crypto.cpp
        include/utils/Crypto.h
        src/utils/System.cpp
        include/utils/System.h
        src/intermediate/BmtGenerator.cpp
        include/intermediate/BmtGenerator.h
        src/compute/arith/ArithLessExecutor.cpp
        include/compute/arith/ArithLessExecutor.h
        src/compute/arith/ArithExecutor.cpp
        include/compute/arith/ArithExecutor.h
        src/api/ArithSecret.cpp
        include/api/ArithSecret.h
        src/compute/arith/ArithMutexExecutor.cpp
        include/compute/arith/ArithMutexExecutor.h
        include/compute/bool/BoolExecutor.h
        src/compute/bool/BoolExecutor.cpp
        include/compute/bool/BoolAndExecutor.h
        src/compute/bool/BoolAndExecutor.cpp
        include/compute/bool/BoolXorExecutor.h
        src/compute/bool/BoolXorExecutor.cpp
        include/compute/bool/BoolEqualExecutor.h
        src/compute/bool/BoolEqualExecutor.cpp
        src/api/BitSecret.cpp
        include/intermediate/item/Bmt.h
        include/comm/Comm.h
        include/intermediate/IntermediateDataSupport.h
        src/intermediate/IntermediateDataSupport.cpp
        src/compute/arith/ArithToBoolExecutor.cpp
        include/compute/arith/ArithToBoolExecutor.h
        src/compute/bool/BoolToArithExecutor.cpp
        include/compute/bool/BoolToArithExecutor.h
        include/intermediate/item/ABPair.h
        src/intermediate/ABPairGenerator.cpp
        include/intermediate/ABPairGenerator.h
        src/comm/Comm.cpp
        include/utils/Log.h
        include/third_party/ctpl_stl.h
        include/sync/LockBlockingQueue.h
        src/compute/bool/BoolLessExecutor.cpp
        include/compute/bool/BoolLessExecutor.h
        src/ot/RandOtExecutor.cpp
        include/ot/RandOtExecutor.h
        include/intermediate/item/SRot.h
        include/intermediate/item/RRot.h
        include/ot/AbstractOtExecutor.h
        src/ot/AbstractOtExecutor.cpp
        include/api/Secret.h
        src/api/Secret.cpp
        src/compute/bool/BoolMutexExecutor.cpp
        include/compute/bool/BoolMutexExecutor.h
        include/api/BoolSecret.h
        src/api/BoolSecret.cpp
        include/sync/BlockingQueue.h
        include/sync/CasBlockingQueue.h
        include/conf/Conf.h
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
        ${PROJECT_NAME} PUBLIC MPI::MPI_CXX OpenSSL::SSL OpenSSL::Crypto
)