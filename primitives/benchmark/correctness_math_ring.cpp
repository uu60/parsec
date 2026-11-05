#include "utils/Math.h"

#include <cstdint>
#include <iostream>
#include <limits>

namespace {
bool expectEqual(const char *name, int64_t actual, int64_t expected) {
    if (actual == expected) {
        return true;
    }
    std::cerr << name << ": expected " << expected << ", got " << actual << '\n';
    return false;
}
}

int main() {
    const int64_t min = std::numeric_limits<int64_t>::min();
    const int64_t max = std::numeric_limits<int64_t>::max();

    bool passed = true;
    passed = expectEqual("ring width 1", Math::ring(-1, 1), 1) && passed;
    passed = expectEqual("64-bit add wrap", Math::ringAdd(max, 1, 64), min) && passed;
    passed = expectEqual("64-bit subtract wrap", Math::ringSubtract(min, 1, 64), max) && passed;
    passed = expectEqual("64-bit multiply wrap", Math::ringMultiply(max, 2, 64), -2) && passed;
    passed = expectEqual("set high bit", Math::changeBit(0, 63, true), min) && passed;
    passed = expectEqual("clear high bit", Math::changeBit(-1, 63, false), max) && passed;
    passed = expectEqual("read high bit", Math::getBit(min, 63), true) && passed;

    if (!passed) {
        return 1;
    }
    std::cout << "[Ring arithmetic correctness] PASS\n";
    return 0;
}
