
# Testing library
FetchContent_Declare(
    catch2
    GIT_REPOSITORY https://github.com/catchorg/Catch2.git
    GIT_TAG        v2.9.1
)
FetchContent_MakeAvailable(catch2)
# Adds Catch2::Catch2

# Tests need to be added as executables first
add_executable(testlib testlib.cpp)

#uncomment the next line to add performance benchmarking to the test
#target_compile_definitions(testlib PUBLIC CATCH_CONFIG_ENABLE_BENCHMARKING)

# I'm using C++17 in the test
target_compile_features(testlib PRIVATE cxx_std_17)

# Should be linked to the main library, as well as the Catch2 testing library
target_link_libraries(testlib PRIVATE alluxio_lib Catch2::Catch2)
find_package(jsoncpp CONFIG REQUIRED)
target_link_libraries(testlib PRIVATE JsonCpp::JsonCpp)

# If you register a test, then ctest and make test will run it.
# You can also run examples and check the output, as well.
add_test(NAME testlibtest COMMAND testlib) # Command can be a target
