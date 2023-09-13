CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
#SET(CMAKE_C_COMPILER "/usr/local/arm/gcc-linaro-7.5.0-2019.12-x86_64_arm-linux-gnueabihf/bin/arm-linux-gnueabihf-gcc")
#SET(CMAKE_C_COMPILER "/usr/local/arm/4.8.3/bin/arm-linux-gcc")
SET(CMAKE_CXX_COMPILER "/usr/bin/g++")
PROJECT(LMdatabase)

SET(CMAKE_C_STANDARD 11)
SET(CMAKE_VERBOSE_MAKEFILE ON)
SET(TD_ROOT_DIR ${PROJECT_SOURCE_DIR})

#
# If it is a Linux operating system
#   1.Generate debug version:
#     mkdir debug; cd debug; 
#     cmake -DCMAKE_BUILD_TYPE=Debug ..
#   2.Generate release version:
#     mkdir release; cd release; 
#     cmake -DCMAKE_BUILD_TYPE=Release .. 
# If it is a Windows operating system
#   1.Use command line tool of VS2013 or higher version
#     mkdir build; cd build;
#     cmake -G "NMake Makefiles" ..
#     nmake install
#   2.Use the VS development interface tool 
#     mkdir build; cd build;
#     cmake -A x64 ..
#     open the file named TDengine.sln
#

# Set macro definitions according to os platform
SET(TD_OS_WINDOWS FALSE)
SET(TD_OS_LINUX TRUE)
SET(TD_OS_ARM TRUE)
SET(TD_OS_MAC FALSE)

IF (${CMAKE_SYSTEM_NAME} MATCHES "Linux")
  IF(TD_OS_ARM)
  		ADD_DEFINITIONS(-DARM)
    #SET(CMAKE_C_COMPILER "/usr/local/arm/gcc-linaro-7.5.0-2019.12-x86_64_arm-linux-gnueabihf/bin/arm-linux-gnueabihf-gcc")
    SET(CMAKE_FIND_ROOT_PATH "/usr/local/arm")
    SET(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
    SET(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)
    SET(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)
    SET(COMMON_FLAGS "-Wall -fPIC -Bsymbolic  -g -Wno-char-subscripts  -D_FILE_OFFSET_BITS=64 -D_LARGE_FILE")
  ELSE ()
    SET(COMMON_FLAGS "-Wall -fPIC -Bsymbolic -malign-double -g -Wno-char-subscripts -malign-stringops -msse4.2 -D_FILE_OFFSET_BITS=64 -D_LARGE_FILE")
  ENDIF()
  
  SET(TD_LINUX TRUE)
  ADD_DEFINITIONS(-DLINUX)
  
  #SET(COMMON_FLAGS "-std=gnu99 -Wall -fPIC -malign-double -g -Wno-char-subscripts -malign-stringops -msse4.2 -D_FILE_OFFSET_BITS=64 -D_LARGE_FILE")
  SET(DEBUG_FLAGS "-O0 -DDEBUG")
  SET(RELEASE_FLAGS "-O0")
  
  ADD_DEFINITIONS(-D_REENTRANT -D__USE_POSIX -D_LIBC_REENTRANT)
  
  IF (${CMAKE_SIZEOF_VOID_P} MATCHES 8)
    MESSAGE(STATUS "The current platform is Linux 64-bit")
    ADD_DEFINITIONS(-D_M_X64)
  ELSE ()
    #MESSAGE(FATAL_ERROR "The current platform is Linux 32-bit, not supported yet")
    #EXIT ()
  ENDIF ()

ELSE ()

  MESSAGE(FATAL_ERROR "The current platform is not Linux, stop compile")
  EXIT ()
  
ENDIF ()

# Set compiler options
SET(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -std=gnu99  ${COMMON_FLAGS} ${DEBUG_FLAGS}")
SET(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} ${COMMON_FLAGS} ${RELEASE_FLAGS}")

# Set c++ compiler options
SET(COMMON_CXX_FLAGS "${COMMON_FLAGS} -std=c++11")
SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} ${COMMON_CXX_FLAGS} ${DEBUG_FLAGS}")
SET(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} ${COMMON_CXX_FLAGS} ${RELEASE_FLAGS}")

IF (${CMAKE_BUILD_TYPE} MATCHES "Debug")
  MESSAGE(STATUS "Build Debug Version")
ELSEIF (${CMAKE_BUILD_TYPE} MATCHES "Release")
  MESSAGE(STATUS "Build Release Version")
ELSE ()
  IF (TD_WINDOWS)
    SET(CMAKE_BUILD_TYPE "Release")
	MESSAGE(STATUS "Build Release Version")
  ELSE ()
    SET(CMAKE_BUILD_TYPE "Debug")
	MESSAGE(STATUS "Build Debug Version")
  ENDIF()    
ENDIF ()

#set output directory
SET(LIBRARY_OUTPUT_PATH ${PROJECT_BINARY_DIR}/build/lib)
SET(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/build/bin)
SET(TD_TESTS_OUTPUT_DIR ${PROJECT_BINARY_DIR}/test)

MESSAGE(STATUS "Project source directory: " ${PROJECT_SOURCE_DIR})
MESSAGE(STATUS "Project binary files output path: " ${PROJECT_BINARY_DIR})
MESSAGE(STATUS "Project executable files output path: " ${EXECUTABLE_OUTPUT_PATH})
MESSAGE(STATUS "Project library files output path: " ${LIBRARY_OUTPUT_PATH})

ADD_SUBDIRECTORY(deps)
ADD_SUBDIRECTORY(src)

INCLUDE(CPack)