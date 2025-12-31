#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <processing.h>

#include <filesystem>
#include <fstream>

namespace fs = std::filesystem;

TEST(SimpleTest, FilterEvenAndSquare) {
    std::vector<int> input = {1, 2, 3, 4, 5, 6, 7, 8};
    auto result = AsDataFlow(input) | Filter([](int x) { return x % 2 == 0; }) | Transform([](int x) { return x * x; }) | AsVector();
    ASSERT_THAT(result, testing::ElementsAre(4, 16, 36, 64));
}

TEST(SimpleTest, DifferentDelimiters) {
    std::vector<std::stringstream> files(2);
    files[0] << "1,2,3,4,5";
    files[1] << "6;7;8;9;10";

    auto result = AsDataFlow(files) | Split(",;") | AsVector();

    ASSERT_THAT(result, testing::ElementsAre("1", "2", "3", "4", "5", "6", "7", "8", "9", "10"));
}

TEST(SimpleTest, WriteOnlyPositiveNumbers) {
    std::vector<int> input = {-2, -1, 0, 1, 2};
    std::stringstream output;

    AsDataFlow(input) | Filter([](int x) { return x > 0; }) | Write(output, ',');

    ASSERT_EQ(output.str(), "1,2,");
}

TEST(SimpleTest, CountLinesInFiles) {
    fs::path test_dir = "test_data";
    fs::create_directories(test_dir);

    std::ofstream(test_dir / "file1.txt") << "line1\nline2\nline3";
    std::ofstream(test_dir / "file2.txt") << "row1\nrow2";

    auto result = Dir(test_dir.string(), false) | OpenFiles() | Split("\n") | AsVector();

    ASSERT_THAT(result, testing::ElementsAre("line1", "line2", "line3", "row1", "row2"));

    fs::remove_all(test_dir);
}

TEST(SimpleTest, SplitAndFilter) {
    std::vector<std::string> input = {
        "hello,world", "test,data,processing", "one,two,three,four"};

    auto result = AsDataFlow(input) | Split(",") | Filter([](const std::string& s) { return s.size() > 3; }) | AsVector();

    ASSERT_THAT(result, testing::ElementsAre("hello", "world", "test", "data", "processing", "three", "four"));
}

TEST(SimpleTest, RunningTotal) {
    std::vector<int> input = {1, 2, 3, 4, 5};
    int total = 0;

    auto result = AsDataFlow(input) | Transform([&total](int x) {
                      total += x;
                      return total;
                  }) |
                  AsVector();

    ASSERT_THAT(result, testing::ElementsAre(1, 3, 6, 10, 15));
}

TEST(SimpleTest, TransformWithSideEffects) {
    std::vector<int> input = {1, 2, 3};
    std::vector<int> side_effects;

    auto result = AsDataFlow(input) | Transform([&side_effects](int x) {
                      side_effects.push_back(x);
                      return x * 10;
                  }) |
                  AsVector();

    ASSERT_THAT(result, testing::ElementsAre(10, 20, 30));
    ASSERT_THAT(side_effects, testing::ElementsAre(1, 2, 3));
}

TEST(SimpleTest, CombinedOperations) {
    std::vector<int> input = {1, 2, 3, 4, 5};
    auto result = AsDataFlow(input) | Transform([](int x) { return x + 10; }) | Filter([](int x) { return x % 2 != 0; }) | AsVector();

    ASSERT_THAT(result, testing::ElementsAre(11, 13, 15));
}

TEST(SimpleTest, MultipleOperations) {
    std::vector<int> input = {1, 2, 3, 4, 5};
    auto result = AsDataFlow(input) | Transform([](int x) { return x + 10; }) | Filter([](int x) { return x % 2 == 1; }) | AsVector();
    ASSERT_THAT(result, testing::ElementsAre(11, 13, 15));
}