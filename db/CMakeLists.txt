file(GLOB_RECURSE CORE_SOURCES CONFIGURE_DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/src/basis/*.cpp")

add_library(db_core STATIC ${CORE_SOURCES})
target_compile_features(db_core PUBLIC cxx_std_17)
target_include_directories(db_core PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_link_libraries(db_core PUBLIC parsec)
