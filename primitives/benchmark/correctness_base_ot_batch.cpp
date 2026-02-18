//
// Created on 2026-02-17.
//

#include "ot/BaseOtBatchOperator.h"
#include "utils/Log.h"
#include "utils/StringUtils.h"
#include "utils/System.h"

int main(int argc, char *argv[]) {
    System::init(argc, argv);

    bool isSender = Comm::rank() == 0;

    // Test data
    std::vector<int64_t> ss0, ss1;
    std::vector<int> choices;

    for (int i = 0; i < 10; i++) {
        ss0.push_back(i * 100);
        ss1.push_back(i * 100 + 50);
        choices.push_back(i % 2);
    }

    // Test BaseOtBatchOperator
    std::vector<int64_t> results = BaseOtBatchOperator(
        0,
        isSender ? &ss0 : nullptr,
        isSender ? &ss1 : nullptr,
        !isSender ? &choices : nullptr,
        64,
        1,
        0
    ).execute()->_results;

    if (Comm::rank() == 1) {
        Log::i("BaseOtBatchOperator results: {}", StringUtils::vecToString(results));

        // Verify results
        for (int i = 0; i < 10; i++) {
            int64_t expected = choices[i] == 0 ? ss0[i] : ss1[i];
            Log::i("OT {}: expected={}, got={}, match={}", i, expected, results[i], expected == results[i]);
        }
    }

    System::finalize();
}


