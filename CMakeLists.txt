#
#  Copyright 2019 - present Proyectos y Sistemas de Mantenimiento SL (eProsima).
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
# is-fastdds SystemHandle library and tests CMake project

###################################################################################
# CMake build rules for the Integration Service Fast DDS SystemHandle library
###################################################################################
cmake_minimum_required(VERSION 3.7.0 FATAL_ERROR)

project(is-fastdds VERSION "3.0.0" LANGUAGES CXX)

###################################################################################
# Configure options
###################################################################################
set (IS_FASTDDS_SH_FASTDDS_TAG 2.3.0
    CACHE STRING "Fast DDS library specific version to download as external project")

option(IS_FASTDDS_SH_USING_FASTDDS_EXTERNALPROJECT
    "Compiling the Fast DDS SystemHandle using Fast DDS as an external project" OFF)

###################################################################################
# Load external CMake Modules.
###################################################################################
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${SANITIZERS_ROOT}/cmake)

find_package(Sanitizers QUIET)

if(SANITIZE_ADDRESS)
    message(STATUS "Preloading AddressSanitizer library could be done using \"${ASan_WRAPPER}\" to run your program.")
endif()

###################################################################################
# External dependencies for the Integration Service Fast DDS SystemHandle library
###################################################################################

include(ExternalProject)

find_package(is-core REQUIRED)
find_package(xtypes REQUIRED)
if(WIN32)
    find_package(yaml-cpp)
endif()

###################################################################################
# Configure the Integration Service Fast DDS SystemHandle library
###################################################################################
add_library(${PROJECT_NAME}
    SHARED
        src/Conversion.cpp
        src/Publisher.cpp
        src/Subscriber.cpp
        src/Client.cpp
        src/Server.cpp
        src/Participant.cpp
        src/SystemHandle.cpp
    )

###################################################################################
# Find FastRTPS dependency. If not, load it as external project
###################################################################################
find_package(fastrtps QUIET)

if(fastrtps_FOUND AND fastrtps_VERSION VERSION_GREATER_EQUAL "2.0.0")
    message(STATUS "Found 'fastrtps ${fastrtps_VERSION}' library in the system: ${fastrtps_DIR}")
    target_link_libraries(${PROJECT_NAME}
        PUBLIC
            is::core
        PRIVATE
            fastrtps
            $<$<PLATFORM_ID:Windows>:yaml-cpp>
    )
else()
    message(STATUS "Downloading and using 'fastrtps ${FASTDDS_VERSION}' as external project...")
    set(IS_FASTDDS_SH_USING_FASTDDS_EXTERNALPROJECT ON)
    # Fast-RTPS needs foonathan too
    unset(foonathan_memory_DIR CACHE) # Reviewer! Do we need to clear CACHE? Copied from uXRCE-DDS-Agent
    find_package(foonathan_memory QUIET)
    if(NOT foonathan_memory_FOUND)
        ExternalProject_Add(foonathan_memory
            GIT_REPOSITORY
                https://github.com/foonathan/memory.git
            GIT_TAG
                main # ${_foonathan_memory_tag} (set(_foonathan_memory_tag master))
            PREFIX
                ${PROJECT_BINARY_DIR}/foonathan_memory
            INSTALL_DIR
                ${PROJECT_BINARY_DIR}/temp_install/foonathan_memory
            CMAKE_CACHE_ARGS
                -DCMAKE_INSTALL_PREFIX:PATH=<INSTALL_DIR>
                -DFOONATHAN_MEMORY_BUILD_EXAMPLES:BOOL=OFF
                -DFOONATHAN_MEMORY_BUILD_TESTS:BOOL=OFF
                -DFOONATHAN_MEMORY_BUILD_TOOLS:BOOL=ON
                -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
                -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
            )
    endif()

    # Fast RTPS.
    unset(fastrtps_DIR CACHE) # Reviewer! Do we need to clear CACHE? Copied from uXRCE-DDS-Agent
    # find_package(fastrtps ${_fastrtps_version} EXACT QUIET) #TODO
    ExternalProject_Add(fastrtps
        GIT_REPOSITORY
            https://github.com/eProsima/Fast-RTPS.git
        GIT_TAG
            "v${IS_FASTDDS_SH_FASTDDS_TAG}"
        PREFIX
            ${PROJECT_BINARY_DIR}/fastrtps
        INSTALL_DIR
            ${PROJECT_BINARY_DIR}/temp_install/fastrtps
        CMAKE_CACHE_ARGS
            -DCMAKE_INSTALL_PREFIX:PATH=<INSTALL_DIR>
            -DCMAKE_PREFIX_PATH:PATH=${CMAKE_PREFIX_PATH};${PROJECT_BINARY_DIR}/temp_install
            -DBUILD_SHARED_LIBS:BOOL=ON
            -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
            -DCMAKE_TOOLCHAIN_FILE:PATH=${CMAKE_TOOLCHAIN_FILE}
            -DTHIRDPARTY:BOOL=ON
            -DSECURITY:BOOL=OFF #${SOSS-DDS_SECURITY_PROFILE}
        DEPENDS
            foonathan_memory
        )

    link_directories(${PROJECT_BINARY_DIR}/temp_install/fastrtps/lib)
    include_directories(${PROJECT_BINARY_DIR}/temp_install/fastrtps/include)
    add_library(libfastrtps IMPORTED SHARED GLOBAL)
    add_library(libfastcdr IMPORTED SHARED GLOBAL)
    add_dependencies(libfastrtps fastrtps)

    set_target_properties(libfastrtps
        PROPERTIES
            "IMPORTED_LOCATION" "${PROJECT_BINARY_DIR}/temp_install/fastrtps/lib/libfastrtps.so"
            "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
        )

    set_target_properties(libfastcdr
        PROPERTIES
            "IMPORTED_LOCATION" "${PROJECT_BINARY_DIR}/temp_install/fastrtps/lib/libfastcdr.so"
            "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
        )

    target_link_libraries(${PROJECT_NAME}
        PUBLIC
            is::core
        PRIVATE
            libfastrtps
            $<$<PLATFORM_ID:Windows>:yaml-cpp>
        )

    install(
        DIRECTORY
            ${PROJECT_BINARY_DIR}/temp_install/fastrtps/lib/
        DESTINATION
            ${CMAKE_INSTALL_LIBDIR}
        COMPONENT
            is-fastdds
        )

    install(
        DIRECTORY
            ${PROJECT_BINARY_DIR}/temp_install/fastrtps/include/
        DESTINATION
            ${CMAKE_INSTALL_INCLUDEDIR}
        COMPONENT
            is-fastdds
        )
endif()

# Library continuation
if(Sanitizers_FOUND)
    add_sanitizers(${PROJECT_NAME})
endif()

set_target_properties(${PROJECT_NAME} PROPERTIES
    VERSION
        ${PROJECT_VERSION}
    SOVERSION
        ${PROJECT_VERSION_MAJOR}.${PROJECT_VERSION_MINOR}
    CXX_STANDARD
        17
    CXX_STANDARD_REQUIRED
        YES
    )

target_compile_options(${PROJECT_NAME}
    PRIVATE
        $<$<CXX_COMPILER_ID:GNU>:-Werror -Wall -Wextra -Wpedantic>
    )

include(GNUInstallDirs)
message(STATUS "Configuring [${PROJECT_NAME}]...")

target_include_directories(${PROJECT_NAME}
    INTERFACE
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>
    )

###################################################################################
# Install the Integration Service FastDDS SystemHandle library
###################################################################################
is_install_middleware_plugin(
    MIDDLEWARE
        fastdds
    TARGET
        is-fastdds
    )

###################################################################################
# Add the Integration Service Fast DDS SystemHandle testing subdirectory
###################################################################################
include(CTest)

if(BUILD_TESTS OR BUILD_FASTDDS_TESTS)
    add_subdirectory(test)
endif()