# Runs exp_1..exp_8 in --check=true mode over MPI and TCP and compares their output with the expected results in
# verify_exp.py.
find_package(Python3 COMPONENTS Interpreter)
if (NOT Python3_Interpreter_FOUND)
    message(STATUS "Python 3 not found - database checks will not be registered")
    return()
endif ()

set(PARSEC_VERIFY_EXP "${CMAKE_CURRENT_SOURCE_DIR}/verify_exp.py")
foreach (exp RANGE 1 8)
    add_test(NAME database.exp_${exp}.mpi
            COMMAND ${Python3_EXECUTABLE} ${PARSEC_VERIFY_EXP} --exp ${exp} --comm mpi
                    --mpirun ${MPIEXEC_EXECUTABLE} "--mpirun-flags=${PARSEC_MPIEXEC_TEST_FLAGS}"
                    --bin-dir $<TARGET_FILE_DIR:exp_${exp}>)
    add_test(NAME database.exp_${exp}.tcp
            COMMAND ${Python3_EXECUTABLE} ${PARSEC_VERIFY_EXP} --exp ${exp} --comm tcp
                    --tcp-base-port 21000 --bin-dir $<TARGET_FILE_DIR:exp_${exp}>)
    set_tests_properties(database.exp_${exp}.mpi PROPERTIES
            LABELS "correctness;database;mpi" PROCESSORS 3 TIMEOUT 180)
    if (PARSEC_MPI_TEST_ENVIRONMENT)
        set_property(TEST database.exp_${exp}.mpi PROPERTY ENVIRONMENT ${PARSEC_MPI_TEST_ENVIRONMENT})
    endif ()
    set_tests_properties(database.exp_${exp}.tcp PROPERTIES
            LABELS "correctness;database;tcp" PROCESSORS 3 TIMEOUT 180)
endforeach ()
