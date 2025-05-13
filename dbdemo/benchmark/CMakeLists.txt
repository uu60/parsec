file(GLOB_RECURSE BENCH_SRCS "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")
foreach(src ${BENCH_SRCS})
    get_filename_component(execName ${src} NAME_WE)
    add_executable(${execName} ${src})
    target_include_directories(${execName} PRIVATE
            ${CMAKE_SOURCE_DIR}/dbdemo/include
    )
    target_link_libraries(${execName} PRIVATE
            dbdemo_core
    )
endforeach()