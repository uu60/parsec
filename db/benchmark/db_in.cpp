//
// Created by 杜建璋 on 25-8-6.
//

#include "secret/Secrets.h"
#include "utils/System.h"

#include "../include/basis/View.h"
#include "basis/Views.h"
#include "utils/Log.h"
#include "utils/StringUtils.h"

#include <string>
#include <random>
#include <algorithm>

int main(int argc, char *argv[]) {
    System::init(argc, argv);

    int col1_size = 100;
    int col2_size = 200;
    int max_value = 50;
    bool test_performance = true;

    // Parse command line arguments
    if (Conf::_userParams.count("col1_size")) {
        col1_size = std::stoi(Conf::_userParams["col1_size"]);
    }
    if (Conf::_userParams.count("col2_size")) {
        col2_size = std::stoi(Conf::_userParams["col2_size"]);
    }
    if (Conf::_userParams.count("max_value")) {
        max_value = std::stoi(Conf::_userParams["max_value"]);
    }

    Log::i("Testing IN function with col1_size={}, col2_size={}, max_value={}", 
           col1_size, col2_size, max_value);

    // Generate test data on client (rank 2)
    std::vector<int64_t> col1_plain, col2_plain;
    std::vector<int64_t> expected_result;
    
    if (Comm::rank() == 2) {
        std::random_device rd;
        std::mt19937 gen(42); // Fixed seed for reproducible results
        std::uniform_int_distribution<int64_t> dis(1, max_value);

        // Generate col1
        col1_plain.resize(col1_size);
        for (int i = 0; i < col1_size; i++) {
            col1_plain[i] = dis(gen);
        }

        // Generate col2 with some overlapping values
        col2_plain.resize(col2_size);
        for (int i = 0; i < col2_size; i++) {
            if (i < col1_size / 2) {
                // Ensure some matches exist
                col2_plain[i] = col1_plain[i * 2];
            } else {
                col2_plain[i] = dis(gen);
            }
        }

        // Calculate expected result (plain computation)
        expected_result.resize(col1_size);
        for (int i = 0; i < col1_size; i++) {
            expected_result[i] = 0;
            for (int j = 0; j < col2_size; j++) {
                if (col1_plain[i] == col2_plain[j]) {
                    expected_result[i] = 1;
                    break;
                }
            }
        }

        Log::i("Generated test data:");
        Log::i("col1: {}", StringUtils::vecToString(col1_plain));
        Log::i("col2: {}", StringUtils::vecToString(col2_plain));
        Log::i("expected: {}", StringUtils::vecToString(expected_result));
    }

    // Convert to secret shares
    auto col1_shares = Secrets::boolShare(col1_plain, 2, 64, System::nextTask());
    auto col2_shares = Secrets::boolShare(col2_plain, 2, 64, System::nextTask());

    std::vector<int64_t> result_shares;
    if (Comm::isServer()) {
        Log::i("Starting performance test...");
        auto start = System::currentTimeMillis();

        // Test IN function
        auto lv = std::vector<int64_t>(col1_shares.size(), Comm::rank());
        auto rv = std::vector<int64_t>(col2_shares.size(), Comm::rank());
        result_shares = Views::in(col1_shares, col2_shares, lv, rv);

        auto end = System::currentTimeMillis();
        Log::i("IN function execution time: {}ms", end - start);
    }

    auto result = Secrets::boolReconstruct(result_shares, 2, 1, System::nextTask());

    if (Comm::rank() == 2) {
        Log::i("actual result: {}", StringUtils::vecToString(result));
    }

    System::finalize();
    return 0;
}
