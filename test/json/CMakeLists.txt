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

file(GLOB_RECURSE SOURCES "src/*.cpp")

add_executable(metadata_test
  ${SOURCES}
)

set_compile_options(metadata_test)

target_include_directories(
  metadata_test
    PRIVATE ${PROJECT_SOURCE_DIR}/include
    PRIVATE include
)

target_link_libraries(metadata_test
  PRIVATE
    gtest_main
    api
)

add_test(NAME test COMMAND metadata_test "${CMAKE_CURRENT_SOURCE_DIR}/src/schema/tables.json")
