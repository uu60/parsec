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

# Public headers live in include/parsec and are included as "parsec/<module>/<Header>.h".
set(PARSEC_INCLUDE_DIR "${PROJECT_SOURCE_DIR}/include")

# parsec: runtime (communication, configuration, parallel support, utilities) and MPC primitives.
set(PARSEC_MODULES
        comm conf utils             # runtime
        accelerate base compute intermediate ot secret system)  # primitives
set(PARSEC_SOURCES "")
foreach (module ${PARSEC_MODULES})
    file(GLOB_RECURSE module_sources CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/${module}/*.cpp")
    list(APPEND PARSEC_SOURCES ${module_sources})
endforeach ()

add_library(parsec STATIC ${PARSEC_SOURCES})
add_library(parsec::parsec ALIAS parsec)
target_compile_features(parsec PUBLIC cxx_std_17)
target_include_directories(parsec PUBLIC
        $<BUILD_INTERFACE:${PARSEC_INCLUDE_DIR}>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
target_link_libraries(parsec PUBLIC MPI::MPI_CXX OpenSSL::SSL OpenSSL::Crypto Boost::boost ${PARSEC_ATOMIC_LIBS})
if (TBB_FOUND)
    target_link_libraries(parsec PUBLIC ${TBB_LIBRARIES})
    # Public headers change with this flag, so consumers must see it too.
    target_compile_definitions(parsec PUBLIC PARSEC_HAS_TBB)
endif ()
install(TARGETS parsec EXPORT parsecTargets)

# parsec::db: secure relational data structures and operators.
if (PARSEC_BUILD_DATABASE)
    file(GLOB_RECURSE PARSEC_DB_SOURCES CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/db/*.cpp")
    add_library(db_core STATIC ${PARSEC_DB_SOURCES})
    add_library(parsec::db ALIAS db_core)
    set_target_properties(db_core PROPERTIES EXPORT_NAME db)
    target_link_libraries(db_core PUBLIC parsec)
    install(TARGETS db_core EXPORT parsecTargets)
endif ()

install(DIRECTORY "${PARSEC_INCLUDE_DIR}/" DESTINATION ${CMAKE_INSTALL_INCLUDEDIR} FILES_MATCHING PATTERN "*.h")
