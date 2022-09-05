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

cmake_minimum_required(VERSION 3.10)

project(manager
  VERSION 0.0.1
  LANGUAGES CXX
)

list(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")

option(BUILD_TESTS "Build test programs" ON)
option(FORCE_INSTALL_RPATH "force add lib dir of custom prefixes to INSTALL_RPATH" OFF)
option(BUILD_DOCUMENTS "Build documents" OFF)

option(ENABLE_SANITIZER "enable sanitizer on debug build" ON)
option(ENABLE_UB_SANITIZER "enable undefined behavior sanitizer on debug build" OFF)
option(ENABLE_COVERAGE "enable coverage on debug build" OFF)

option(DATA_STORAGE "Specifies the data-storage where the metadata is stored. [postgresql] or json." POSTGRESQL)

find_package(Doxygen)
find_package(Boost COMPONENTS system filesystem REQUIRED)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
include(CompileOptions)
include(InstallOptions)
include(DatabaseOptions)

if(BUILD_TESTS)
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

string(TOUPPER ${DATA_STORAGE} VARNAME)

if((VARNAME STREQUAL "POSTGRESQL") OR(NOT DATA_STORAGE))
  set(DATA_STORAGE_PG TRUE)
elseif(VARNAME STREQUAL "JSON")
  set(DATA_STORAGE_JSON TRUE)
else()
  unset(DATA_STORAGE CACHE)
  message(FATAL_ERROR "Unsupported data storage. [${VARNAME}]")
endif()

unset(DATA_STORAGE CACHE)

add_custom_target(format
  COMMAND git ls-files '*.cpp' '*.h' | xargs clang-format -i --style=file -verbose
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

set(SUBMODULE_DIR ${CMAKE_SOURCE_DIR}/third_party)
set(JWT-CPP_INCLUDE_DIR ${SUBMODULE_DIR}/jwt-cpp/include)

add_library(metadata-interface INTERFACE)

add_subdirectory(${SUBMODULE_DIR}/googletest)

message(STATUS "The target of the build is metadata-manager.")
if(DATA_STORAGE_PG)
  message(STATUS "  Metadata is stored in PostgreSQL.")
  target_compile_options(metadata-interface INTERFACE -DSTORAGE_POSTGRESQL)
  set_postgresql()
elseif(DATA_STORAGE_JSON)
  message(STATUS "  Metadata is stored in JSON-file.")
  target_compile_options(metadata-interface INTERFACE -DSTORAGE_JSON)
endif(DATA_STORAGE_PG)

target_include_directories(metadata-interface
  INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/manager>
)

install_custom(metadata-interface ${export_name})

add_subdirectory(src)
add_subdirectory(test)

if(BUILD_DOCUMENTS)
  add_subdirectory(doxygen)
endif()
unset(BUILD_DOCUMENTS CACHE)
