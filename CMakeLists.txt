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

cmake_minimum_required(VERSION 3.0)

project(manager
        VERSION 0.0.1
        LANGUAGES CXX
)

list(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")

option(BUILD_TESTS "Build test programs" ON)
option(FORCE_INSTALL_RPATH "force add lib dir of custom prefixes to INSTALL_RPATH" OFF)
option(BUILD_DOCUMENTS "Build documents" ON)

option(ENABLE_SANITIZER "enable sanitizer on debug build" ON)
option(ENABLE_UB_SANITIZER "enable undefined behavior sanitizer on debug build" OFF)
option(ENABLE_COVERAGE "enable coverage on debug build" OFF)

find_package(Doxygen)
find_package(Boost COMPONENTS system filesystem REQUIRED)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
include(CompileOptions)
include(InstallOptions)

if (BUILD_TESTS)
    enable_testing()
endif()

set(package_name "manager")
set(export_name "manager")

configure_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/Config.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/${package_name}-config.cmake
    @ONLY
)

write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/${package_name}-config-version.cmake"
    COMPATIBILITY SameMajorVersion
)

install(
    FILES
        ${CMAKE_CURRENT_BINARY_DIR}/${package_name}-config.cmake
        ${CMAKE_CURRENT_BINARY_DIR}/${package_name}-config-version.cmake
    DESTINATION
        ${CMAKE_INSTALL_LIBDIR}/cmake/${package_name}
)

install(
    EXPORT ${package_name}
    NAMESPACE ${package_name}-
    FILE ${package_name}-targets.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/${package_name}
    EXPORT_LINK_INTERFACE_LIBRARIES
)

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

FIND_PROGRAM(PG_CONFIG pg_config)

IF (PG_CONFIG)
   SET(PG_CONFIG_FOUND TRUE)
ENDIF (PG_CONFIG)

IF (PG_CONFIG_FOUND)
    MESSAGE(STATUS "Found pg_config for PostgreSQL: ${PG_CONFIG}")
    get_pg_config(PG_INCLUDEDIR --includedir)
    get_pg_config(PG_LIBDIR --libdir)
ELSE (NOT DEFINED PKG_CONFIG_FOUND)
    MESSAGE(FATAL_ERROR "pg_config not found in path")
    MESSAGE(FATAL_ERROR "cannot build")
ENDIF (PG_CONFIG_FOUND)

add_custom_target(format
  COMMAND           git ls-files '*.cpp' '*.h' | xargs clang-format -i --style=file -verbose
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

add_subdirectory(message-broker)
add_subdirectory(metadata-manager)
add_subdirectory(third_party/googletest)

if (BUILD_DOCUMENTS)
    add_subdirectory(doxygen)
endif()
