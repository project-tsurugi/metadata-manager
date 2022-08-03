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

project(metadata_test)

file(GLOB_RECURSE SOURCES "src/*.cpp")

add_executable(${PROJECT_NAME}
  ${SOURCES}
)

set_compile_options(${PROJECT_NAME})

target_include_directories(
  ${PROJECT_NAME}
  PRIVATE ${PG_INCLUDE_DIR}
  PRIVATE ${JWT-CPP_INCLUDE_DIR}
  PRIVATE ${PROJECT_SOURCE_DIR}/include/manager/metadata
  PRIVATE include
)

set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -L${PG_LIB_DIR}")

target_link_libraries(${PROJECT_NAME}
  PRIVATE
    gtest_main
    metadata
    crypto
    pq
)

add_test(NAME test COMMAND ${PROJECT_NAME} "${CMAKE_CURRENT_SOURCE_DIR}/src/schema/tables.json")
