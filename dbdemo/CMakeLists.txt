file(GLOB_RECURSE CORE_SOURCES
        "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp"
)
list(FILTER CORE_SOURCES EXCLUDE REGEX "${CMAKE_CURRENT_SOURCE_DIR}/client/.*")
set(HSQL_LIB "${CMAKE_CURRENT_SOURCE_DIR}/../libsqlparser.so")
add_library(dbdemo_core STATIC ${CORE_SOURCES})
target_include_directories(dbdemo_core PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_link_libraries(dbdemo_core PUBLIC secure_2pc_package ${HSQL_LIB})


add_executable(dbdemo
        "${CMAKE_CURRENT_SOURCE_DIR}/src/main.cpp"
)
target_link_libraries(dbdemo PRIVATE dbdemo_core)

find_library(READLINE_LIBRARY NAMES readline)
if (NOT READLINE_LIBRARY)
    message(FATAL_ERROR "readline library not found.")
endif()

add_executable(dbdemo_client ${CMAKE_CURRENT_SOURCE_DIR}/client/main.cpp)

# Link readline to the executable
target_link_libraries(dbdemo_client ${READLINE_LIBRARY})