
cmake_minimum_required(VERSION 3.0)

project(manager_example
        VERSION 0.0.1
        LANGUAGES CXX
)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

set(CMAKE_CXX_COMPILER /usr/bin/g++)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_FLAGS "-O2")
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g -Wall -Wextra")

set(package_name "manager_example")
set(export_name "manager_example")

# Add an executable
add_executable(manager_example main.cpp oltp_receiver.cpp worker.cpp)

find_package(manager REQUIRED)
target_link_libraries(manager_example
    PRIVATE manager-metadata
)
