add_library(metadata
    SHARED
        metadata.cpp table_metadata.cpp datatype_metadata.cpp object_id.cpp
)

target_include_directories(metadata
    PUBLIC ${PROJECT_SOURCE_DIR}/../..
)

set_target_properties(metadata
    PROPERTIES
        LIBRARY_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}/output"
)
