# Copyright 2019-2020 tsurugi project.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -L${PG_LIB_DIR}")

add_executable(roles_test roles_test.cpp)
target_link_libraries(roles_test metadata pq)
target_include_directories(roles_test
  PRIVATE ${PG_INCLUDE_DIR}
  PRIVATE ${PROJECT_SOURCE_DIR}/include/manager/metadata
  PRIVATE include
)
set_target_properties(roles_test
  PROPERTIES
  RUNTIME_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}/output"
)

add_executable(tables_test tables_test.cpp)
target_link_libraries(tables_test metadata pq)
target_include_directories(tables_test
  PRIVATE ${PG_INCLUDE_DIR}
  PRIVATE ${PROJECT_SOURCE_DIR}/include/manager/metadata
  PRIVATE include
)
set_target_properties(tables_test
  PROPERTIES
  RUNTIME_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}/output"
)
