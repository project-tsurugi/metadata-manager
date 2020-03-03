add_library(metadata
    SHARED
        metadata.cpp table_metadata.cpp datatype_metadata.cpp object_id.cpp
)

target_include_directories(metadata
<<<<<<< HEAD:metadata/mock/CMakeLists.txt
    PUBLIC ${PROJECT_SOURCE_DIR}/../..
=======
    PRIVATE ${PROJECT_SOURCE_DIR}/include/manager/metadata
>>>>>>> 1e552dabb99d0a9c40f2168fa7016ac438918144:metadata-manager/mock/CMakeLists.txt
)

set_target_properties(metadata
    PROPERTIES
        LIBRARY_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}/output"
)

target_link_libraries(metadata
    PUBLIC metadata-interface
)

install_custom(metadata ${export_name})
