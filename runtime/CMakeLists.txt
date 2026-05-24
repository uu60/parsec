set(PARSEC_RUNTIME_INCLUDE_DIRS
        "${CMAKE_CURRENT_SOURCE_DIR}/config/include"
        "${CMAKE_CURRENT_SOURCE_DIR}/comm/include"
        "${CMAKE_CURRENT_SOURCE_DIR}/parallel/include"
        "${CMAKE_CURRENT_SOURCE_DIR}/utils/include")

function(parsec_add_runtime_object target)
    cmake_parse_arguments(PARSEC_RUNTIME "" "" "SOURCES;LIBS" ${ARGN})

    add_library(${target} OBJECT ${PARSEC_RUNTIME_SOURCES})
    target_compile_features(${target} PUBLIC cxx_std_17)
    target_include_directories(${target} PUBLIC ${PARSEC_RUNTIME_INCLUDE_DIRS})
    target_link_libraries(${target} PUBLIC Boost::boost)

    if (TBB_FOUND)
        target_link_libraries(${target} PUBLIC ${TBB_LIBRARIES})
    endif ()

    if (PARSEC_RUNTIME_LIBS)
        target_link_libraries(${target} PUBLIC ${PARSEC_RUNTIME_LIBS})
    endif ()
endfunction()

file(GLOB_RECURSE PARSEC_CONFIG_SOURCES CONFIGURE_DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/config/src/*.cpp")
file(GLOB_RECURSE PARSEC_COMM_SOURCES CONFIGURE_DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/comm/src/*.cpp")
file(GLOB_RECURSE PARSEC_UTILS_SOURCES CONFIGURE_DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/utils/src/*.cpp")

parsec_add_runtime_object(parsec_config
        SOURCES ${PARSEC_CONFIG_SOURCES})
parsec_add_runtime_object(parsec_comm
        SOURCES ${PARSEC_COMM_SOURCES}
        LIBS MPI::MPI_CXX)
parsec_add_runtime_object(parsec_utils
        SOURCES ${PARSEC_UTILS_SOURCES}
        LIBS OpenSSL::SSL OpenSSL::Crypto)

add_library(parsec_parallel INTERFACE)
target_compile_features(parsec_parallel INTERFACE cxx_std_17)
target_include_directories(parsec_parallel INTERFACE ${PARSEC_RUNTIME_INCLUDE_DIRS})
target_link_libraries(parsec_parallel INTERFACE Boost::boost)
if (TBB_FOUND)
    target_link_libraries(parsec_parallel INTERFACE ${TBB_LIBRARIES})
endif ()

set(PARSEC_RUNTIME_INCLUDE_DIRS ${PARSEC_RUNTIME_INCLUDE_DIRS} PARENT_SCOPE)
set(PARSEC_RUNTIME_OBJECT_MODULES
        parsec_config
        parsec_comm
        parsec_utils
        PARENT_SCOPE)
set(PARSEC_RUNTIME_LINK_LIBS
        parsec_parallel
        MPI::MPI_CXX
        OpenSSL::SSL
        OpenSSL::Crypto
        Boost::boost
        PARENT_SCOPE)
