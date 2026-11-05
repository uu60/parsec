file(GLOB_RECURSE EXP_SRCS "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")
foreach (src ${EXP_SRCS})
    get_filename_component(execName ${src} NAME_WE)
    add_executable(${execName} ${src})
    target_include_directories(${execName} PRIVATE
            ${CMAKE_SOURCE_DIR}/db/include
    )
    target_link_libraries(${execName} PRIVATE
            db_core
    )
endforeach ()

if (PARSEC_BUILD_TESTS)
    find_package(Python3 COMPONENTS Interpreter)
    if (Python3_Interpreter_FOUND)
        set(PARSEC_VERIFY_EXP "${CMAKE_CURRENT_SOURCE_DIR}/correctness/verify_exp.py")
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
            set_tests_properties(database.exp_${exp}.tcp PROPERTIES
                    LABELS "correctness;database;tcp" PROCESSORS 3 TIMEOUT 180)
        endforeach ()
    endif ()
endif ()
