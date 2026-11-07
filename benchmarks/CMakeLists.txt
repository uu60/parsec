# primitives/  MPC primitive and OT benchmarks
# database/    secure database operator benchmarks (needs PARSEC_BUILD_DATABASE)
add_subdirectory(primitives)
if (PARSEC_BUILD_DATABASE)
    add_subdirectory(database)
endif ()
