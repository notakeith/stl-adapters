include(FetchContent)

FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG release-1.12.1
)

set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

enable_testing()

add_executable(
    processing-lib-tests
    drop_nullopt_ut.cpp
    filter_ut.cpp
    flow_convertions_ut.cpp
    join_ut.cpp
    aggregate_by_key_ut.cpp
    split_expected_ut.cpp
    split_ut.cpp
    transform_ut.cpp
    write_ut.cpp
    files_ut.cpp
    self_made_ut.cpp
)

target_link_libraries(
    processing-lib-tests
    GTest::gtest_main
    GTest::gmock_main
)

target_include_directories(processing-lib-tests PUBLIC ${PROJECT_SOURCE_DIR})

include(GoogleTest)

gtest_discover_tests(processing-lib-tests)
