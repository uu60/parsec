file(GLOB_RECURSE CORE_SOURCES CONFIGURE_DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/src/basis/*.cpp")

add_library(db_core STATIC ${CORE_SOURCES})
add_library(parsec::db ALIAS db_core)
set_target_properties(db_core PROPERTIES EXPORT_NAME db)
target_compile_features(db_core PUBLIC cxx_std_17)
target_include_directories(db_core PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${PARSEC_INSTALL_INCLUDEDIR}>)
target_link_libraries(db_core PUBLIC parsec)

install(TARGETS db_core EXPORT parsecTargets)
install(DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/include/" DESTINATION ${PARSEC_INSTALL_INCLUDEDIR}
        FILES_MATCHING PATTERN "*.h")
