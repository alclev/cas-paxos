cmake_minimum_required(VERSION 3.18)
project(cas-paxos LANGUAGES CXX VERSION 1.0.0)

# Options
option(DOCS "Set to build documentation" OFF)

# Flags
set(CXX_STANDARD 20 CACHE STRING "Uses C++20 or 23")

# Mandate BUILD_MODE Cmake flag
if(NOT DEFINED BUILD_MODE)
  message(FATAL_ERROR "You must specify BUILD_MODE (e.g., -DBUILD_MODE=Debug or Release)")
endif()

# Set to Debug or Release mode
if(BUILD_MODE STREQUAL "DEBUG")
  message(STATUS "Building in Debug mode")
  set(CMAKE_BUILD_TYPE Debug)
  add_compile_options(-g -O0)
  add_compile_options(-U ROMULUS_LOG_LEVEL -D ROMULUS_LOG_LEVEL=DEBUG)
  # add_compile_definitions(LOG_DUMP)
elseif(BUILD_MODE STREQUAL "RELEASE")
  message(STATUS "Building in Release mode")
  set(CMAKE_BUILD_TYPE Release)
  add_compile_options(-O3)
  add_compile_options(-U ROMULUS_LOG_LEVEL -D ROMULUS_LOG_LEVEL=RELEASE)
else()
  message(FATAL_ERROR "BUILD_MODE must be: (DEBUG | RELEASE)")
endif()

# Set global compiler flags
add_compile_options(-Wall -Wextra -Wwrite-strings -Werror)
#######################################################################################################################
# Setup
set(CMAKE_CXX_STANDARD_REQUIRED ON)
message(STATUS "Using standard ${CXX_STANDARD}")

set(CMAKE_CXX_STANDARD ${CXX_STANDARD})
set(ROMULUS_INCLUDE_PATH "/home/alclev/remus/include")

add_library(${PROJECT_NAME} INTERFACE)
target_include_directories(${PROJECT_NAME}
  INTERFACE
  ${CMAKE_SOURCE_DIR}/include
  ${ROMULUS_INCLUDE_PATH}
)

option(USE_MU "Enable Mu" OFF)

add_subdirectory(experiments)

