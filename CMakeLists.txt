cmake_minimum_required(VERSION 3.14)
project(my_project)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
)

FetchContent_MakeAvailable(googletest)

enable_testing()

add_executable(
  ssearch_test.out
  tests/ssearch_test.cpp
)

target_link_libraries(
  ssearch_test.out
  GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(ssearch_test.out)


# add_test(NAME ssearch_test COMMAND ssearch_test.out)
# set_tests_properties(ssearch_test PROPERTIES TIMEOUT 5)
