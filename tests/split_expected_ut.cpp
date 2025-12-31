#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <processing.h>

#include <expected>

struct Department {
    std::string name;
    bool operator==(const Department& other) const {
        return name == other.name;
    }
};

std::expected<Department, std::string> MakeDepartment(const std::string& str) {
    if (str.empty()) {
        return std::unexpected("Department name is empty");
    }
    if (str.contains(' ')) {
        return std::unexpected("Department name contains space");
    }
    return Department{str};
}

TEST(SplitExpectedTest, SplitExpected) {
    std::vector<std::expected<Department, std::string>> departments;
    departments.emplace_back(MakeDepartment("good-department"));
    departments.emplace_back(MakeDepartment("bad department"));
    departments.emplace_back(MakeDepartment(""));
    departments.emplace_back(MakeDepartment("another-good-department"));

    auto split_result = AsDataFlow(departments) | SplitExpected();

    std::stringstream unexpected_file;
    std::move(split_result.unexpected_stream) | Write(unexpected_file, '.');

    auto expected_result = std::move(split_result.expected_stream) | AsVector();

    ASSERT_EQ(unexpected_file.str(), "Department name contains space.Department name is empty.");
    ASSERT_THAT(expected_result, testing::ElementsAre(
                                     Department{"good-department"},
                                     Department{"another-good-department"}));
}