#include "parsec/compute/batch/bool/BoolEqualBatchOperator.h"
#include "parsec/compute/single/bool/BoolEqualOperator.h"
#include "parsec/utils/Log.h"
#include "parsec/utils/Math.h"
#include "parsec/utils/System.h"

#include <array>
#include <cstdint>
#include <vector>

using namespace parsec;

namespace {
bool testBatchWidth(int width) {
    constexpr int count = 17;
    std::vector<int64_t> xs;
    std::vector<int64_t> ys;
    std::vector<int64_t> expected;

    if (Comm::isClient()) {
        xs.reserve(count);
        ys.reserve(count);
        expected.reserve(count);
        for (int i = 0; i < count; ++i) {
            const int64_t x = Math::ring(static_cast<int64_t>(i) * 0x12345 + 7, width);
            const bool equal = i % 3 == 0;
            const int64_t y = equal ? x : Math::changeBit(x, i % width, !Math::getBit(x, i % width));
            xs.push_back(x);
            ys.push_back(y);
            expected.push_back(equal ? 1 : 0);
        }
    }

    BoolEqualBatchOperator op(&xs, &ys, width, System::nextTask(), 0, 2);
    op.execute()->reconstruct(2);

    if (!Comm::isClient()) {
        return true;
    }

    if (op._results.size() != expected.size()) {
        Log::e("BoolEqual batch width={} result size: expected={}, got={}", width, expected.size(), op._results.size());
        return false;
    }
    for (size_t i = 0; i < expected.size(); ++i) {
        if (op._results[i] != expected[i]) {
            Log::e("BoolEqual batch MISMATCH width={} index={} expected={} got={}", width, i, expected[i],
                   op._results[i]);
            return false;
        }
    }
    return true;
}

bool testSingleWidth(int width) {
    bool passed = true;
    for (int i = 0; i < 3; ++i) {
        const int64_t x = Math::ring(static_cast<int64_t>(i) * 97 + 5, width);
        const bool equal = i == 0;
        const int64_t y = equal ? x : Math::changeBit(x, i % width, !Math::getBit(x, i % width));

        BoolEqualOperator op(x, y, width, System::nextTask(), 0, 2);
        op.execute()->reconstruct(2);
        if (Comm::isClient() && op._result != (equal ? 1 : 0)) {
            Log::e("BoolEqual single MISMATCH width={} case={} expected={} got={}", width, i, equal ? 1 : 0,
                   op._result);
            passed = false;
        }
    }
    return passed;
}
} // namespace

int main(int argc, char** argv) {
    System::init(argc, argv);
    bool passed = true;

    for (int width : std::array<int, 9>{1, 2, 3, 7, 8, 31, 32, 63, 64}) {
        passed = testBatchWidth(width) && passed;
    }
    for (int width : std::array<int, 3>{1, 7, 64}) {
        passed = testSingleWidth(width) && passed;
    }

    if (Comm::isClient()) {
        Log::i(passed ? "[BoolEqual correctness] PASS" : "[BoolEqual correctness] FAIL");
    }
    System::finalize();
    return passed ? 0 : 1;
}
