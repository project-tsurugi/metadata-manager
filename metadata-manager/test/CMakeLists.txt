add_executable(table_test table_test.cpp)

target_link_libraries(table_test metadata)

target_include_directories(table_test
    PRIVATE ${PROJECT_SOURCE_DIR}/include/manager/metadata
)

set_target_properties(table_test
    PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}/output"
)
