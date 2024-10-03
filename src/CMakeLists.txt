
# Note that headers are optional, and do not affect add_library, but they will not
# show up in IDEs unless they are listed in add_library.
file(GLOB HEADER_LIST CONFIGURE_DEPENDS "${PROJECT_SOURCE_DIR}/include/alluxio_lib/*.hpp")

# Make an automatic library - will be static or dynamic based on user setting
add_library(alluxio_lib lib.cpp ${HEADER_LIST})

# We need this directory, and users of our library will need it too
target_include_directories(alluxio_lib PUBLIC ../include)

# This depends on (header only) boost
find_package(Boost REQUIRED COMPONENTS uuid)
target_link_libraries(alluxio_lib PRIVATE Boost::uuid)
find_package(jsoncpp CONFIG REQUIRED)
target_link_libraries(alluxio_lib PRIVATE JsonCpp::JsonCpp)
find_package(etcd-cpp-api CONFIG REQUIRED)
target_link_libraries(alluxio_lib PRIVATE etcd-cpp-api)
find_package(murmurhash CONFIG REQUIRED)
target_link_libraries(alluxio_lib PRIVATE murmurhash::murmurhash)
find_package(fmt CONFIG REQUIRED)
target_link_libraries(alluxio_lib PRIVATE fmt::fmt)

# All users of this library will need at least C++11
target_compile_features(alluxio_lib PUBLIC cxx_std_11)

# IDEs should put the headers in a nice place
#source_group(TREE "${PROJECT_SOURCE_DIR}/include" PREFIX "Header Files" FILES ${HEADER_LIST})
