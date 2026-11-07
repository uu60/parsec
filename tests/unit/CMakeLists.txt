# Fast, single-process unit tests (no MPI ranks). Requires GoogleTest; the tests are skipped when it is not found.
find_package(GTest CONFIG QUIET)
if (NOT GTest_FOUND)
    find_package(GTest QUIET)
endif ()
if (NOT TARGET GTest::gtest_main)
    message(STATUS "GoogleTest not found - unit tests will not be built")
    return()
endif ()

include(GoogleTest)

add_executable(parsecdb_unit_tests
        crypto_test.cpp
        math_test.cpp
        simd_test.cpp
        string_utils_test.cpp)
target_link_libraries(parsecdb_unit_tests PRIVATE parsecdb::parsecdb GTest::gtest_main)
gtest_discover_tests(parsecdb_unit_tests
        TEST_PREFIX "unit."
        PROPERTIES LABELS unit TIMEOUT 30
        DISCOVERY_TIMEOUT 60)
