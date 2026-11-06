#include "utils/StringUtils.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace {

TEST(StringUtils, HasPrefix) {
    EXPECT_TRUE(StringUtils::hasPrefix("--comm_type=tcp", "--comm"));
    EXPECT_TRUE(StringUtils::hasPrefix("abc", ""));
    EXPECT_FALSE(StringUtils::hasPrefix("ab", "abc"));
    EXPECT_FALSE(StringUtils::hasPrefix("xabc", "abc"));
}

TEST(StringUtils, VecToString) {
    EXPECT_EQ(StringUtils::vecToString(std::vector<int>{}), "[]");
    EXPECT_EQ(StringUtils::vecToString(std::vector<int>{1}), "[1]");
    EXPECT_EQ(StringUtils::vecToString(std::vector<int>{1, -2, 3}), "[1, -2, 3]");
}

} // namespace
