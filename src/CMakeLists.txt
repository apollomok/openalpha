cmake_minimum_required(VERSION 3.10.0)
project(openalpha)

if(NOT CMAKE_BUILD_TYPE)
	set(CMAKE_BUILD_TYPE Release)
endif()
	
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -export-dynamic")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-deprecated-declarations -std=c++17")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

find_path(LOG4CXX_HPP_INCLUDE_PATH log4cxx/logger.h)
find_library(LOG4CXX_LIBRARY_PATH log4cxx)
if(LOG4CXX_HPP_INCLUDE_PATH AND LOG4CXX_LIBRARY_PATH)
  message(STATUS "Found log4cxx")
  include_directories(${LOG4CXX_HPP_INCLUDE_PATH})
else()
  message(FATAL_ERROR "log4cxx not found.")
endif()

find_package(PythonInterp 3 REQUIRED)
find_package(PythonLibs 3 REQUIRED)
include_directories(${PYTHON_INCLUDE_DIRS})
link_libraries(${PYTHON_LIBRARIES}) # Deprecated but so convenient!

message(STATUS ${PYTHON_EXECUTABLE})
execute_process(COMMAND ${PYTHON_EXECUTABLE} -c "import numpy as np; print(np.get_include())" OUTPUT_VARIABLE PY_NUMPY)
if (NOT PY_NUMPY)
  message(FATAL_ERROR "numpy not found.")
endif()
string(STRIP ${PY_NUMPY} PY_NUMPY)
include_directories(${PY_NUMPY})

find_package(Boost REQUIRED COMPONENTS program_options filesystem python3 numpy3) 
include_directories({Boost_INCLUDE_DIRS})

add_subdirectory(openalpha)
add_subdirectory(alpha)
