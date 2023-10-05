# Copyright 2019-2020 Project Tsurugi.
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

file(GLOB_RECURSE SOURCES "*.cpp")

if (DATA_STORAGE_JSON)
  message(STATUS "  Exclude postgresql sources.")
  list(FILTER SOURCES EXCLUDE REGEX "/?postgresql/*")
  else()
  message(STATUS "  Exclude json sources.")
  list(FILTER SOURCES EXCLUDE REGEX "/?json/*")
endif()

add_library(api
  SHARED ${SOURCES}
)

target_include_directories(api
  PRIVATE ${PG_INCLUDE_DIR}
  PRIVATE ${JWT-CPP_INCLUDE_DIR}
  PRIVATE ${PROJECT_SOURCE_DIR}/include
)

set_target_properties(api
  PROPERTIES
  LIBRARY_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}"
  LIBRARY_OUTPUT_NAME "metadata-manager"
)

if (DATA_STORAGE_PG)
  set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -L${PG_LIB_DIR}")
  target_link_libraries(api
    PRIVATE pq
  )
endif()

target_link_libraries(api
  PUBLIC
    ${Boost_FILESYSTEM_LIBRARY}
    ${Boost_SYSTEM_LIBRARY}
    interface
  PRIVATE 
    crypto
)

install_custom(api ${export_name})
