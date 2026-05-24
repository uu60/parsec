set(PARSEC_PRIMITIVES_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/include")
set(PARSEC_ALL_INCLUDE_DIRS
        ${PARSEC_PRIMITIVES_INCLUDE_DIR}
        ${PARSEC_RUNTIME_INCLUDE_DIRS})

function(parsec_add_primitive_object target)
    cmake_parse_arguments(PARSEC_PRIMITIVE "" "" "SOURCES;LIBS" ${ARGN})

    add_library(${target} OBJECT ${PARSEC_PRIMITIVE_SOURCES})
    target_compile_features(${target} PUBLIC cxx_std_17)
    target_include_directories(${target} PUBLIC ${PARSEC_ALL_INCLUDE_DIRS})
    target_link_libraries(${target} PUBLIC ${PARSEC_RUNTIME_LINK_LIBS})

    if (TBB_FOUND)
        target_link_libraries(${target} PUBLIC ${TBB_LIBRARIES})
    endif ()

    if (PARSEC_PRIMITIVE_LIBS)
        target_link_libraries(${target} PUBLIC ${PARSEC_PRIMITIVE_LIBS})
    endif ()
endfunction()

file(GLOB_RECURSE PARSEC_ACCELERATE_SOURCES CONFIGURE_DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/src/accelerate/*.cpp")
file(GLOB_RECURSE PARSEC_BASE_SOURCES CONFIGURE_DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/src/base/*.cpp")
file(GLOB_RECURSE PARSEC_COMPUTE_SOURCES CONFIGURE_DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/src/compute/*.cpp")
file(GLOB_RECURSE PARSEC_INTERMEDIATE_SOURCES CONFIGURE_DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/src/intermediate/*.cpp")
file(GLOB_RECURSE PARSEC_OT_SOURCES CONFIGURE_DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/src/ot/*.cpp")
file(GLOB_RECURSE PARSEC_SECRET_SOURCES CONFIGURE_DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/src/secret/*.cpp")
file(GLOB_RECURSE PARSEC_SYSTEM_SOURCES CONFIGURE_DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/src/system/*.cpp")

parsec_add_primitive_object(parsec_accelerate
        SOURCES ${PARSEC_ACCELERATE_SOURCES})
parsec_add_primitive_object(parsec_base
        SOURCES ${PARSEC_BASE_SOURCES})
parsec_add_primitive_object(parsec_compute
        SOURCES ${PARSEC_COMPUTE_SOURCES}
        LIBS MPI::MPI_CXX)
parsec_add_primitive_object(parsec_intermediate
        SOURCES ${PARSEC_INTERMEDIATE_SOURCES})
parsec_add_primitive_object(parsec_ot
        SOURCES ${PARSEC_OT_SOURCES})
parsec_add_primitive_object(parsec_secret
        SOURCES ${PARSEC_SECRET_SOURCES})
parsec_add_primitive_object(parsec_system
        SOURCES ${PARSEC_SYSTEM_SOURCES})

set(PARSEC_PRIMITIVE_OBJECT_MODULES
        parsec_accelerate
        parsec_base
        parsec_compute
        parsec_intermediate
        parsec_ot
        parsec_secret
        parsec_system)

add_library(parsec STATIC)
target_compile_features(parsec PUBLIC cxx_std_17)
target_include_directories(parsec PUBLIC ${PARSEC_ALL_INCLUDE_DIRS})

foreach (module ${PARSEC_RUNTIME_OBJECT_MODULES} ${PARSEC_PRIMITIVE_OBJECT_MODULES})
    target_sources(parsec PRIVATE $<TARGET_OBJECTS:${module}>)
endforeach ()

target_link_libraries(parsec PUBLIC ${PARSEC_RUNTIME_LINK_LIBS})
if (TBB_FOUND)
    target_link_libraries(parsec PUBLIC ${TBB_LIBRARIES})
endif ()

add_subdirectory(benchmark)
