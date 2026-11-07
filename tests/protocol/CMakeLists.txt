# Three-party protocol correctness tests. Every program in this directory is built and registered with CTest over
# MPI and, for most, over the TCP backend. A test passes when every rank exits with status 0 and no rank prints
# FAIL or MISMATCH.
file(GLOB PARSEC_PROTOCOL_TEST_SOURCES CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")
foreach (src ${PARSEC_PROTOCOL_TEST_SOURCES})
    get_filename_component(name ${src} NAME_WE)
    add_executable(${name} ${src})
    target_link_libraries(${name} PRIVATE parsecdb)
endforeach ()

function(parsec_add_mpi_test name target)
    cmake_parse_arguments(PARSEC_TEST "SLOW" "" "ARGS" ${ARGN})
    add_test(
            NAME ${name}
            COMMAND ${MPIEXEC_EXECUTABLE} ${PARSEC_MPIEXEC_TEST_FLAGS_LIST} ${MPIEXEC_NUMPROC_FLAG} 3
                    ${MPIEXEC_PREFLAGS} $<TARGET_FILE:${target}>
                    ${PARSEC_TEST_ARGS} ${MPIEXEC_POSTFLAGS})
    set_tests_properties(${name} PROPERTIES
            LABELS "correctness;protocol;mpi"
            PROCESSORS 3
            TIMEOUT 120
            FAIL_REGULAR_EXPRESSION "FAIL;MISMATCH")
    if (PARSEC_MPI_TEST_ENVIRONMENT)
        set_property(TEST ${name} PROPERTY ENVIRONMENT ${PARSEC_MPI_TEST_ENVIRONMENT})
    endif ()
    if (PARSEC_TEST_SLOW)
        set_property(TEST ${name} APPEND PROPERTY LABELS slow)
        set_property(TEST ${name} PROPERTY TIMEOUT 300)
    endif ()
endfunction()

find_package(Python3 COMPONENTS Interpreter)
set(PARSEC_TCP_TEST_PORT 19000)

# Runs the same three-rank test over the TCP backend as independent processes. Each test gets its own port
# range so tests can run in parallel.
function(parsec_add_tcp_test name target)
    cmake_parse_arguments(PARSEC_TEST "SLOW" "" "ARGS" ${ARGN})
    if (NOT Python3_Interpreter_FOUND)
        return()
    endif ()
    math(EXPR port "${PARSEC_TCP_TEST_PORT} + 10")
    set(PARSEC_TCP_TEST_PORT ${port} PARENT_SCOPE)
    add_test(
            NAME ${name}
            COMMAND ${Python3_EXECUTABLE} ${PROJECT_SOURCE_DIR}/tools/run_tcp_ranks.py
                    --base-port ${port} --timeout 100 $<TARGET_FILE:${target}> -- ${PARSEC_TEST_ARGS})
    set_tests_properties(${name} PROPERTIES
            LABELS "correctness;protocol;tcp"
            PROCESSORS 3
            TIMEOUT 120)
    if (PARSEC_TEST_SLOW)
        set_property(TEST ${name} APPEND PROPERTY LABELS slow)
    endif ()
endfunction()

parsec_add_mpi_test(correctness.ot correctness_ot)
parsec_add_mpi_test(correctness.base_ot_batch correctness_base_ot_batch)
parsec_add_mpi_test(correctness.bitwise_bmt correctness_bitwise_bmt)
parsec_add_mpi_test(correctness.iknp_ot correctness_iknp_ot_simple)
parsec_add_mpi_test(correctness.iknp_ot_bits correctness_iknp_ot_forbits_simple)
parsec_add_mpi_test(correctness.iknp_ot_bits_both_directions correctness_iknp_ot_forbits_both_directions)
parsec_add_mpi_test(correctness.bmt_ot_logic correctness_bmt_ot_logic)
parsec_add_mpi_test(correctness.bool_equal correctness_bool_equal)
parsec_add_mpi_test(correctness.bool_equal_fixed correctness_bool_equal ARGS --bmt_method=bmt_fixed)
parsec_add_mpi_test(correctness.secret_sort_arith correctness_secrets_sort SLOW ARGS --test_mode=arith)
parsec_add_mpi_test(correctness.secret_sort_bool correctness_secrets_sort SLOW ARGS --test_mode=bool)

parsec_add_tcp_test(correctness.tcp.ot correctness_ot)
parsec_add_tcp_test(correctness.tcp.base_ot_batch correctness_base_ot_batch)
parsec_add_tcp_test(correctness.tcp.bitwise_bmt correctness_bitwise_bmt)
parsec_add_tcp_test(correctness.tcp.iknp_ot correctness_iknp_ot_simple)
parsec_add_tcp_test(correctness.tcp.bmt_ot_logic correctness_bmt_ot_logic)
parsec_add_tcp_test(correctness.tcp.bool_equal correctness_bool_equal)
parsec_add_tcp_test(correctness.tcp.secret_sort_arith correctness_secrets_sort SLOW ARGS --test_mode=arith)
