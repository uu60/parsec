file(GLOB_RECURSE CORE_SOURCES
        "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp"
)
list(FILTER CORE_SOURCES EXCLUDE REGEX "/(main|debugParser)\\.cpp$")

set(PARSEC_SQLPARSER_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third_party/sql-parser")
file(GLOB_RECURSE PARSEC_SQLPARSER_SOURCES CONFIGURE_DEPENDS
        "${PARSEC_SQLPARSER_DIR}/src/*.cpp")
add_library(parsec_sqlparser STATIC ${PARSEC_SQLPARSER_SOURCES})
target_compile_features(parsec_sqlparser PUBLIC cxx_std_17)
target_include_directories(parsec_sqlparser PUBLIC
        "${PARSEC_SQLPARSER_DIR}/src")

add_library(db_core STATIC ${CORE_SOURCES})
target_include_directories(db_core PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_link_libraries(db_core PUBLIC parsec parsec_sqlparser)


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

if (PARSEC_BUILD_EXPERIMENTS)
    add_subdirectory(exp)
endif ()
