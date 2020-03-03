add_library(metadata
    SHARED
        metadata.cpp table_metadata.cpp datatype_metadata.cpp object_id.cpp
)

target_include_directories(metadata
    PRIVATE ${PROJECT_SOURCE_DIR}/include/manager/metadata
)

set_target_properties(metadata
    PROPERTIES
        LIBRARY_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}/output"
)

target_link_libraries(metadata
    PUBLIC metadata-interface
)

install_custom(metadata ${export_name})
