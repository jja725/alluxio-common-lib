# alluxio-lib
An native C++ library of Alluxio built using modern cmake approach as described


## Dependencies

* CMake 3.15 or better
* A C++ compatible compiler
* vcpkg (https://vcpkg.io/en/getting-started.html)

## Visual Studio Code Remote Containers Support

This project includes support for developing in a docker container using the 
Visual Studio Code Remote - Containers extension.  The configured docker container
includes everything needed to build the project so you don't have to deal with
installing the dependencies above

# to configure
``` bash
cmake -DCMAKE_BUILD_TYPE=Debug \
      -DCMAKE_TOOLCHAIN_FILE={SOURCE_DIR}/vcpkg/scripts/buildsystems/vcpkg.cmake \
      -G Ninja \
      -B build
```

# to build
``` bash
cmake --build build
```

# to test
``` bash
cmake --build build --target test
```

# configure, build and run tests
``` bash
cmake -S . -B build && cmake --build build && cmake --build build -t test -- -e CTEST_OUTPUT_ON_FAILURE=1
```