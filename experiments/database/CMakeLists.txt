# Research experiments exp_1..exp_8. Their --check=true mode is verified by tests/database.
file(GLOB PARSEC_EXPERIMENT_SOURCES CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")
foreach (src ${PARSEC_EXPERIMENT_SOURCES})
    get_filename_component(name ${src} NAME_WE)
    add_executable(${name} ${src})
    target_link_libraries(${name} PRIVATE parsec::db)
endforeach ()
