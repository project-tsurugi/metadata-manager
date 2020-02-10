add_executable(tester tester.cpp)

target_link_libraries(tester metadata)

set_target_properties(tester
    PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}/output"
)

