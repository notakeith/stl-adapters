#include "processing.h" 

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <filesystem>
#include <fstream>
#include <sstream>

namespace fs = std::filesystem;

void create_test_files(const fs::path& dir) {
    fs::create_directories(dir / "subdir");
    std::ofstream(dir / "file1.txt") << "Content of file1\n";
    std::ofstream(dir / "file2.txt") << "Content of file2\n";
    std::ofstream(dir / "subdir/file3.txt") << "Content of file3\n";
}

TEST(DirTest, ReadFiles) {
    fs::path test_dir = "tests/testfiles";
    create_test_files(test_dir);

    std::ostringstream output;
    Dir(test_dir.string(), true) | OpenFiles() | Out(output);

    EXPECT_THAT(
        output.str(),
        ::testing::HasSubstr("Content of file1"));
    EXPECT_THAT(
        output.str(),
        ::testing::HasSubstr("Content of file2"));
    EXPECT_THAT(
        output.str(),
        ::testing::HasSubstr("Content of file3"));

    fs::remove_all(test_dir);
}

TEST(DirTest, ReadFilesNonRecursive) {
    fs::path test_dir = "tests/testfiles";
    create_test_files(test_dir);

    std::ostringstream output;
    Dir(test_dir.string(), false) | OpenFiles() | Out(output);

    EXPECT_THAT(
        output.str(),
        ::testing::HasSubstr("Content of file1"));
    EXPECT_THAT(
        output.str(),
        ::testing::HasSubstr("Content of file2"));
    EXPECT_THAT(
        output.str(),
        ::testing::Not(::testing::HasSubstr("Content of file3")));

    fs::remove_all(test_dir);
}

TEST(DirTest, EmptyDirectory) {
    fs::path test_dir = "tests/emptydir";
    fs::create_directories(test_dir);

    std::ostringstream output;
    Dir(test_dir.string(), true) | OpenFiles() | Out(output);

    EXPECT_TRUE(output.str().empty());

    fs::remove_all(test_dir);
}

TEST(DirTest, NonExistentDirectory) {
    fs::path test_dir = "tests/nonexistentdir";

    EXPECT_THROW(
        Dir(test_dir.string(), true) | OpenFiles() | Out(std::cout),
        std::runtime_error);
}


TEST(DirTest, NestedDirectoryStructure) {
    fs::path test_dir = "test_dir";
    fs::create_directories(test_dir / "sub1" / "sub2");
    std::ofstream(test_dir / "file1.txt") << "file1";
    std::ofstream(test_dir / "sub1/file2.txt") << "file2";
    std::ofstream(test_dir / "sub1/sub2/file3.txt") << "file3";

    std::stringstream output;
    Dir(test_dir.string(), true) | OpenFiles() | Out(output);

    ASSERT_THAT(output.str(), "file1file2file3");
    fs::remove_all(test_dir);
}
