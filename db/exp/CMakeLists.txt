file(GLOB_RECURSE EXP_SRCS "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")
foreach(src ${EXP_SRCS})
    get_filename_component(execName ${src} NAME_WE)
    add_executable(${execName} ${src})
    target_include_directories(${execName} PRIVATE
            ${CMAKE_SOURCE_DIR}/db/include
    )
    target_link_libraries(${execName} PRIVATE
            db_core
    )
endforeach()
