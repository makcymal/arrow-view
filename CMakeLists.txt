cmake_minimum_required(VERSION 3.16)


# CMake runtime

if(NOT CMAKE_BINARY_DIR STREQUAL ${CMAKE_SOURCE_DIR}/.cmake)
  message(FATAL_ERROR "Run \"cmake -B .cmake\"")
endif()

# set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/../.build)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/..)


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

set(CMAKE_CXX_FLAGS "-Wall")

if(NOT DEFINED CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 14)
endif()

set(CMAKE_CXX_STANDARD_REQUIRED ON)


# External packages

find_package(Arrow REQUIRED)
find_package(Boost REQUIRED COMPONENTS program_options)


# executable targets

file(GLOB
  arrow_view_IMPL
  CONFIGURE_DEPENDS
  "src/lib/tabulate.hpp"
  "src/impl/*.h"
  "src/impl/*.cc"
)

add_executable(arrow-view src/arrow_view.cc ${arrow_view_IMPL})
target_link_libraries(arrow-view PRIVATE Arrow::arrow_shared ${Boost_LIBRARIES})
