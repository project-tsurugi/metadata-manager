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

add_library(metadata
    SHARED
        metadata.cpp object_id.cpp tables.cpp datatypes.cpp
)

target_include_directories(metadata
    PRIVATE ${PROJECT_SOURCE_DIR}/include/manager/metadata
)

set_target_properties(metadata
    PROPERTIES
        LIBRARY_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}/output"
        LIBRARY_OUTPUT_NAME "manager-metadata"
)

target_link_libraries(metadata
    PUBLIC metadata-interface
    ${Boost_FILESYSTEM_LIBRARY}
    ${Boost_SYSTEM_LIBRARY}
)

install_custom(metadata ${export_name})
