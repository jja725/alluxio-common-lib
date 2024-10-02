cmake_minimum_required(VERSION 3.15)

project(AlluxioLib 
        VERSION 0.1.0
        DESCRIPTION "Alluxio Native Library"
        LANGUAGES CXX)

# Set the C++ standard
set(CMAKE_CXX_STANDARD 11)

# set the build type if not specified
set(default_build_type "Release")
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message(STATUS "Setting build type to '${default_build_type}' as none was specified.")
  set(CMAKE_BUILD_TYPE "${default_build_type}")
endif()


# FetchContent to include MurmurHash3
include(FetchContent)

FetchContent_Declare(
  murmurhash3
  GIT_REPOSITORY https://github.com/aappleby/smhasher.git
  GIT_TAG master
)

FetchContent_MakeAvailable(murmurhash3)

include(FetchContent)
FetchContent_Declare(
  etcd-cpp-apiv3
  GIT_REPOSITORY https://github.com/etcd-cpp-apiv3/etcd-cpp-apiv3.git
)
FetchContent_MakeAvailable(etcd-cpp-apiv3)


# Only do these if this is the main project, and not if it is included through add_subdirectory
if(CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME)

    # Let's nicely support folders in IDE's
    set_property(GLOBAL PROPERTY USE_FOLDERS ON)

    # Testing only available if this is the main app
    # Note this needs to be done in the main CMakeLists
    # since it calls enable_testing, which must be in the
    # main CMakeLists.
    include(CTest)

endif()

include(FetchContent)

# The compiled library code is here
add_subdirectory(src)

# The executable code is here
add_subdirectory(apps)

