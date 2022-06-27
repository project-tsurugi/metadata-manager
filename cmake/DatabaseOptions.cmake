# Copyright 2018-2019 tsurugi project.
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

function(GET_PG_CONFIG var)
  set(_temp)

  if(NOT ${var})
    execute_process(
      COMMAND ${PG_CONFIG} ${ARGN}
      OUTPUT_VARIABLE _temp
      OUTPUT_STRIP_TRAILING_WHITESPACE)
  endif()

  set(${var} ${_temp} PARENT_SCOPE)
endfunction()

function(SET_POSTGRESQL)
  FIND_PROGRAM(PG_CONFIG pg_config)

  if(PG_CONFIG)
    set(PG_CONFIG_FOUND TRUE)
  endif(PG_CONFIG)

  if(PG_CONFIG_FOUND)
    MESSAGE(STATUS "Found pg_config for PostgreSQL: ${PG_CONFIG}")
  
    get_pg_config(_temp --includedir)
    set(PG_INCLUDE_DIR ${_temp} PARENT_SCOPE)
    MESSAGE(STATUS "  Header file directory: ${_temp}")

    get_pg_config(_temp --libdir)
    set(PG_LIB_DIR ${_temp} PARENT_SCOPE)
    MESSAGE(STATUS "  Library file directory: ${_temp}")

  else(NOT DEFINED PKG_CONFIG_FOUND)
    MESSAGE(FATAL_ERROR "pg_config not found in path")
    MESSAGE(FATAL_ERROR "cannot build")
  endif(PG_CONFIG_FOUND)
endfunction(SET_POSTGRESQL)
