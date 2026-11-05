file(GLOB_RECURSE BENCH_SRCS "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")
foreach (src ${BENCH_SRCS})
    get_filename_component(execName ${src} NAME_WE)

    if (execName MATCHES "^correctness_")
        if (NOT PARSEC_BUILD_TESTS AND NOT PARSEC_BUILD_BENCHMARKS)
            continue()
        endif ()
    elseif (NOT PARSEC_BUILD_BENCHMARKS)
        continue()
    endif ()

    add_executable(${execName} ${src})
    target_link_libraries(${execName} PRIVATE parsec)
endforeach ()

if (PARSEC_BUILD_TESTS)
    function(parsec_add_mpi_test name target)
        add_test(
                NAME ${name}
                COMMAND ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} 3
                        ${MPIEXEC_PREFLAGS} $<TARGET_FILE:${target}> ${MPIEXEC_POSTFLAGS})
        set_tests_properties(${name} PROPERTIES
                LABELS "correctness;mpi"
                PROCESSORS 3
                TIMEOUT 120)
    endfunction()

    parsec_add_mpi_test(correctness.bitwise_bmt correctness_bitwise_bmt)
    set_tests_properties(correctness.bitwise_bmt PROPERTIES
            PASS_REGULAR_EXPRESSION "\\[BitwiseBmt correctness\\] PASS"
            FAIL_REGULAR_EXPRESSION "FAIL;MISMATCH")
endif ()
