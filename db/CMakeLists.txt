file(GLOB_RECURSE CORE_SOURCES
        "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp"
)
list(FILTER CORE_SOURCES EXCLUDE REGEX "${CMAKE_CURRENT_SOURCE_DIR}/client/.*")
set(HSQL_LIB "${CMAKE_CURRENT_SOURCE_DIR}/../libsqlparser.so")
add_library(db_core STATIC ${CORE_SOURCES})
target_include_directories(db_core PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_link_libraries(db_core PUBLIC parsec ${HSQL_LIB})


add_executable(db
        "${CMAKE_CURRENT_SOURCE_DIR}/src/main.cpp"
)
target_link_libraries(db PRIVATE db_core)

find_library(READLINE_LIBRARY NAMES readline)
if (NOT READLINE_LIBRARY)
    message(FATAL_ERROR "readline library not found.")
endif ()

add_executable(db_client ${CMAKE_CURRENT_SOURCE_DIR}/client/main.cpp)

# Link readline to the executable
target_link_libraries(db_client ${READLINE_LIBRARY})

add_executable(db_debugSql "${CMAKE_CURRENT_SOURCE_DIR}/src/debugParser.cpp")

target_link_libraries(db_debugSql PRIVATE db_core)

add_subdirectory(exp)
