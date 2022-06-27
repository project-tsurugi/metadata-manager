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
option(BUILD_TARGET "Specifies the target for the build. [ALL] or METADATA or AUTH." ALL)
option(AUTHENTICATE "Specifies the method used for authentication. [postgresql]." POSTGRESQL)

find_package(Doxygen)
find_package(Boost COMPONENTS system filesystem REQUIRED)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
include(CompileOptions)
include(InstallOptions)
include(DatabaseOptions)

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

string(TOUPPER ${BUILD_TARGET} VARNAME)
if((VARNAME STREQUAL "ALL") OR (NOT BUILD_TARGET))
  set(BUILD_TARGET_META TRUE)
  set(BUILD_TARGET_AUTH TRUE)
elseif(VARNAME STREQUAL "METADATA")
  set(BUILD_TARGET_META TRUE)
  set(BUILD_TARGET_AUTH FALSE)
elseif(VARNAME STREQUAL "AUTH")
  set(BUILD_TARGET_META FALSE)
  set(BUILD_TARGET_AUTH TRUE)
else()
  unset(BUILD_TARGET CACHE)
  message(FATAL_ERROR "Unsupported build targer. [${VARNAME}]")
endif()
unset(BUILD_TARGET CACHE)

if(BUILD_TARGET_AUTH)
  string(TOUPPER ${AUTHENTICATE} VARNAME)
  if((VARNAME STREQUAL "POSTGRESQL") OR (NOT AUTHENTICATE))
    set(AUTHENTICATE_PG TRUE)
    set(USE_DB_POSTGRESQL TRUE)
  else()
    unset(AUTHENTICATE_PG CACHE)
    message(FATAL_ERROR "Unsupported authentication method. [${VARNAME}]")
  endif()
  unset(AUTHENTICATE CACHE)
endif(BUILD_TARGET_AUTH)

if(BUILD_TARGET_META)
  string(TOUPPER ${DATA_STORAGE} VARNAME)
  if((VARNAME STREQUAL "POSTGRESQL") OR (NOT DATA_STORAGE))
    set(DATA_STORAGE_PG TRUE)
    set(USE_DB_POSTGRESQL TRUE)
  elseif(VARNAME STREQUAL "JSON")
    set(DATA_STORAGE_JSON TRUE)
  else()
    unset(DATA_STORAGE CACHE)
    message(FATAL_ERROR "Unsupported data storage. [${VARNAME}]")
  endif()
  unset(DATA_STORAGE CACHE)
endif(BUILD_TARGET_META)

if(USE_DB_POSTGRESQL)
  set_postgresql()
endif(USE_DB_POSTGRESQL)

add_custom_target(format
    COMMAND
      git ls-files '*.cpp' '*.h' | xargs clang-format -i --style=file -verbose
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

set(SUBMODULE_DIR ${CMAKE_SOURCE_DIR}/third_party)
set(JWT-CPP_INCLUDE_DIR ${SUBMODULE_DIR}/jwt-cpp/include)

add_subdirectory(message-broker)
add_subdirectory(${SUBMODULE_DIR}/googletest)

if(BUILD_TARGET_META)
  message(STATUS "The target of the build is metadata-manager.")
  add_subdirectory(metadata-manager)
endif(BUILD_TARGET_META)

if(BUILD_TARGET_AUTH)
  message(STATUS "The target of the build is authentication-manager.")
  add_subdirectory(authentication-manager)
endif(BUILD_TARGET_AUTH)

if(BUILD_DOCUMENTS)
  if(!BUILD_TARGET_META OR !BUILD_TARGET_AUTH)
    message(FATAL_ERROR "The document must be created with BUILD_TARGET=ALL.")
  else()
    add_subdirectory(doxygen)
  endif()
endif()
unset(BUILD_DOCUMENTS CACHE)
