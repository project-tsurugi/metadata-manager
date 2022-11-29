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
list(FILTER SOURCES EXCLUDE REGEX "/?.not_build/*")

add_executable(metadata_test
  ${SOURCES}
)

set_compile_options(metadata_test)

target_include_directories(
  metadata_test
    PRIVATE ${PG_INCLUDE_DIR}
    PRIVATE ${JWT-CPP_INCLUDE_DIR}
    PRIVATE ${PROJECT_SOURCE_DIR}/include
    PRIVATE include
)

set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -L${PG_LIB_DIR}")

target_link_libraries(metadata_test
  PRIVATE
    gtest_main
    api
    crypto
    pq
)

add_test(NAME test COMMAND metadata_test "${CMAKE_CURRENT_SOURCE_DIR}/src/schema/tables.json")
