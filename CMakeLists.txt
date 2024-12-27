cmake_minimum_required(VERSION 3.16)


# CMake runtime

if(NOT CMAKE_BINARY_DIR STREQUAL ${CMAKE_SOURCE_DIR}/.cmake)
  message(FATAL_ERROR "Run \"cmake -B .cmake\"")
endif()

# set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/../.build)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/../.build)


# Project

project(arrow-view
  VERSION 0.1.0
  DESCRIPTION "CLI preview of Apache Arrow files"
  LANGUAGES CXX
)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()


# Setting up C++

set(CMAKE_CXX_FLAGS "-Wall -Werror")

if(NOT DEFINED CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 17)
endif()

set(CMAKE_CXX_STANDARD_REQUIRED ON)


# External libraries

find_package(Arrow REQUIRED)


# executable targets

add_executable(arrow-head src/arrow_head.cc src/utils.cc src/utils.h)
target_link_libraries(arrow-head PRIVATE Arrow::arrow_shared)

add_executable(arrow-info src/arrow_info.cc src/utils.cc src/utils.h)
target_link_libraries(arrow-info PRIVATE Arrow::arrow_shared)
target_link_libraries(arrow-head PRIVATE Arrow::arrow_shared)

add_executable(arrow-desc src/arrow_desc.cc src/utils.cc src/utils.h)
target_link_libraries(arrow-desc PRIVATE Arrow::arrow_shared)
