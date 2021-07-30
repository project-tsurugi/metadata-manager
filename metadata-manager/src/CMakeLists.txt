# Copyright 2019-2020 tsurugi project.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

project(metadata)

file(GLOB_RECURSE SOURCES "*.cpp")

if (DATA_STORAGE_JSON)
    list(FILTER SOURCES EXCLUDE REGEX "/?postgresql/*")
else()
    list(FILTER SOURCES EXCLUDE REGEX "/?json/*")
endif()
list(FILTER SOURCES EXCLUDE REGEX "/?datatype_metadata.cpp$")
list(FILTER SOURCES EXCLUDE REGEX "/?table_metadata.cpp$")

add_library(${PROJECT_NAME}
    SHARED ${SOURCES}
)

target_include_directories(${PROJECT_NAME}
    PRIVATE ${PROJECT_SOURCE_DIR}/include/manager/metadata
    PRIVATE ${PG_INCLUDEDIR}
)

set_target_properties(${PROJECT_NAME}
    PROPERTIES
        LIBRARY_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}/output"
        LIBRARY_OUTPUT_NAME "manager-metadata"
)

if (DATA_STORAGE_PG)
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -L${PG_LIBDIR}")
endif (DATA_STORAGE_PG)

target_link_libraries(${PROJECT_NAME}
    PUBLIC ${PROJECT_NAME}-interface
    ${Boost_FILESYSTEM_LIBRARY}
    ${Boost_SYSTEM_LIBRARY}
    PRIVATE pq
)

install_custom(${PROJECT_NAME} ${export_name})
