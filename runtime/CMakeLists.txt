include(CheckCXXSourceCompiles)

# Boost.Lockfree queues of 16-byte BMT records need double-width compare-and-swap. Some toolchains (for example
# recent Clang on Linux) lower that to __atomic_compare_exchange, which lives in libatomic.
set(PARSEC_ATOMIC_TEST_SOURCE [=[
#include <atomic>
#include <cstdint>
struct Wide { std::int64_t a; std::int64_t b; };
int main() {
    std::atomic<Wide> value{Wide{0, 0}};
    Wide expected = value.load();
    value.compare_exchange_strong(expected, Wide{1, 1});
    return static_cast<int>(value.load().a);
}
]=])
set(CMAKE_REQUIRED_FLAGS "-std=c++17")
check_cxx_source_compiles("${PARSEC_ATOMIC_TEST_SOURCE}" PARSEC_HAS_BUILTIN_WIDE_ATOMICS)
set(PARSEC_ATOMIC_LIBS "")
if (NOT PARSEC_HAS_BUILTIN_WIDE_ATOMICS)
    set(CMAKE_REQUIRED_LIBRARIES atomic)
    check_cxx_source_compiles("${PARSEC_ATOMIC_TEST_SOURCE}" PARSEC_HAS_LIBATOMIC_WIDE_ATOMICS)
    unset(CMAKE_REQUIRED_LIBRARIES)
    if (PARSEC_HAS_LIBATOMIC_WIDE_ATOMICS)
        set(PARSEC_ATOMIC_LIBS atomic)
    else ()
        message(FATAL_ERROR "16-byte std::atomic operations are unavailable, even with libatomic")
    endif ()
endif ()
unset(CMAKE_REQUIRED_FLAGS)

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
    target_link_libraries(${target} PUBLIC Boost::boost ${PARSEC_ATOMIC_LIBS})

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
set_target_properties(parsec_parallel PROPERTIES EXPORT_NAME parallel)
target_compile_features(parsec_parallel INTERFACE cxx_std_17)
parsec_build_include_dirs(PARSEC_RUNTIME_BUILD_INCLUDE_DIRS ${PARSEC_RUNTIME_INCLUDE_DIRS})
target_include_directories(parsec_parallel INTERFACE
        ${PARSEC_RUNTIME_BUILD_INCLUDE_DIRS}
        $<INSTALL_INTERFACE:${PARSEC_INSTALL_INCLUDEDIR}>)
target_link_libraries(parsec_parallel INTERFACE Boost::boost)
if (TBB_FOUND)
    target_link_libraries(parsec_parallel INTERFACE ${TBB_LIBRARIES})
    # Public headers change with this flag, so consumers must see it too.
    target_compile_definitions(parsec_parallel INTERFACE PARSEC_HAS_TBB)
endif ()
install(TARGETS parsec_parallel EXPORT parsecTargets)
foreach (dir ${PARSEC_RUNTIME_INCLUDE_DIRS})
    install(DIRECTORY "${dir}/" DESTINATION ${PARSEC_INSTALL_INCLUDEDIR} FILES_MATCHING PATTERN "*.h")
endforeach ()

set(PARSEC_RUNTIME_INCLUDE_DIRS ${PARSEC_RUNTIME_INCLUDE_DIRS} PARENT_SCOPE)
set(PARSEC_RUNTIME_OBJECT_MODULES
        parsec_config
        parsec_comm
        parsec_utils
        PARENT_SCOPE)
set(PARSEC_RUNTIME_LINK_LIBS
        ${PARSEC_ATOMIC_LIBS}
        parsec_parallel
        MPI::MPI_CXX
        OpenSSL::SSL
        OpenSSL::Crypto
        Boost::boost
        PARENT_SCOPE)
