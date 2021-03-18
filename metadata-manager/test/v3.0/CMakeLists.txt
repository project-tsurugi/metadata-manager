project(metadata_test)

file(GLOB_RECURSE SOURCES "src/*.cpp")

add_executable(${PROJECT_NAME}
  ${SOURCES}
)

set_compile_options(${PROJECT_NAME})

target_include_directories(
    ${PROJECT_NAME}
    PRIVATE ${PG_INCLUDEDIR}
    PRIVATE ${PROJECT_SOURCE_DIR}/include/manager/metadata
    PRIVATE include
)

set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -L${PG_LIBDIR}")

target_link_libraries(${PROJECT_NAME}
  PRIVATE
    gtest_main
    metadata
    pq
)

add_test(NAME test COMMAND ${PROJECT_NAME})
