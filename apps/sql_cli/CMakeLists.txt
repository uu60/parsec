set(PARSEC_SQLPARSER_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third_party/sql-parser")
file(GLOB_RECURSE PARSEC_SQLPARSER_SOURCES CONFIGURE_DEPENDS
        "${PARSEC_SQLPARSER_DIR}/src/*.cpp")
add_library(parsec_sqlparser STATIC ${PARSEC_SQLPARSER_SOURCES})
target_compile_features(parsec_sqlparser PUBLIC cxx_std_17)
target_include_directories(parsec_sqlparser PUBLIC
        "${PARSEC_SQLPARSER_DIR}/src")

file(GLOB_RECURSE PARSEC_SQL_CLI_SOURCES CONFIGURE_DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/src/dbms/*.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/operator/*.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/socket/*.cpp")
add_library(parsec_sql_cli_core STATIC ${PARSEC_SQL_CLI_SOURCES})
target_compile_features(parsec_sql_cli_core PUBLIC cxx_std_17)
target_include_directories(parsec_sql_cli_core PUBLIC
        "${CMAKE_CURRENT_SOURCE_DIR}/include")
target_link_libraries(parsec_sql_cli_core PUBLIC db_core parsec_sqlparser)

add_executable(db "${CMAKE_CURRENT_SOURCE_DIR}/src/main.cpp")
target_link_libraries(db PRIVATE parsec_sql_cli_core)

find_library(READLINE_LIBRARY NAMES readline)
if (NOT READLINE_LIBRARY)
    message(FATAL_ERROR "readline library not found; set PARSEC_BUILD_SQL_CLI=OFF to build only the database core")
endif ()

add_executable(db_client "${CMAKE_CURRENT_SOURCE_DIR}/client/main.cpp")
target_link_libraries(db_client PRIVATE ${READLINE_LIBRARY})
target_compile_definitions(db_client PRIVATE
        PARSEC_SQL_CLI_PRECMD_FILE="${CMAKE_CURRENT_SOURCE_DIR}/client/precmd")

add_executable(db_debugSql "${CMAKE_CURRENT_SOURCE_DIR}/src/debugParser.cpp")
target_link_libraries(db_debugSql PRIVATE parsec_sql_cli_core)
