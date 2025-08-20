//
// Created by 杜建璋 on 25-5-2.
//

#include "secret/Secrets.h"
#include "utils/System.h"

#include "../include/basis/View.h"
#include "../include/operator/SelectSupport.h"
#include "basis/Views.h"
#include "utils/Log.h"
#include "utils/StringUtils.h"

#include <string>

int main(int argc, char *argv[]) {
    System::init(argc, argv);

    // Create test data that clearly demonstrates multi-column sorting
    // Data format: (col1, col2) - should sort by col1 DESC, then col2 DESC
    std::vector<std::pair<int64_t, int64_t>> testData = {
        {3, 1},  // Should be 3rd after sorting (3,3), (3,2), (3,1)
        {1, 3},  // Should be 6th after sorting  
        {2, 2},  // Should be 5th after sorting
        {3, 3},  // Should be 1st after sorting (highest in both columns)
        {1, 1},  // Should be 8th after sorting (lowest in both columns)
        {2, 3},  // Should be 4th after sorting
        {3, 2},  // Should be 2nd after sorting
        {1, 2}   // Should be 7th after sorting
    };

    // Convert to shares
    std::vector<int64_t> col1_shares, col2_shares;
    if (Comm::rank() == 2) {
        for (const auto& pair : testData) {
            col1_shares.push_back(pair.first);
            col2_shares.push_back(pair.second);
        }
    } else {
        col1_shares.resize(testData.size(), 0);
        col2_shares.resize(testData.size(), 0);
    }

    col1_shares = Secrets::boolShare(col1_shares, 2, 64, System::nextTask());
    col2_shares = Secrets::boolShare(col2_shares, 2, 64, System::nextTask());

    View v;
    if (Comm::isServer()) {
        std::string name = "demo";
        std::vector<std::string> fn = {"a0", "a1"};
        std::vector<int> ws = {64, 64};

        Table t(name, fn, ws, "a0");
        for (int i = 0; i < col1_shares.size(); i++) {
            std::vector<int64_t> r = {col1_shares[i], col2_shares[i], 0}; // 0 for key field
            t.insert(r);
        }

        v = Views::selectAll(t);

        // Print original data (only on one server to avoid duplication)
        if (Comm::rank() == 0) {
            Log::i("Original data (col1, col2):");
            for (size_t i = 0; i < testData.size(); i++) {
                Log::i("Row {}: ({}, {})", i, testData[i].first, testData[i].second);
            }
            Log::i("Expected sorted order (DESC, DESC): (3,3), (3,2), (3,1), (2,3), (2,2), (1,3), (1,2), (1,1)");
        }

        auto start = System::currentTimeMillis();
        std::vector<std::string> orderFields = {"a0", "a1"};
        std::vector ascendingOrders = {false, false}; // Both descending
        v.sort(orderFields, ascendingOrders, 0);
        Log::i("Sort time: {}ms", System::currentTimeMillis() - start);
    }

    // Reconstruct and display results once for both display and verification
    std::vector<int64_t> col1_result, col2_result;
    
    auto col1_secrets = Comm::isServer() ? v._dataCols[0] : std::vector<int64_t>();
    auto col2_secrets = Comm::isServer() ? v._dataCols[1] : std::vector<int64_t>();
    col1_result = Secrets::boolReconstruct(col1_secrets, 2, 64, System::nextTask());
    col2_result = Secrets::boolReconstruct(col2_secrets, 2, 64, System::nextTask());

    if (Comm::rank() == 2) {
        Log::i("Column 0: {}", StringUtils::vecToString(col1_result));
        Log::i("Column 1: {}", StringUtils::vecToString(col2_result));
    }
    
    if (Comm::rank() == 2) {
        Log::i("Sorted result verification:");
        for (size_t i = 0; i < col1_result.size(); i++) {
            Log::i("Row {}: ({}, {})", i, col1_result[i], col2_result[i]);
        }
        
        // Check if sorting is correct (both columns descending)
        bool sortingCorrect = true;
        for (size_t i = 0; i < col1_result.size() - 1; i++) {
            // For descending order: current should be >= next
            if (col1_result[i] < col1_result[i + 1] || 
                (col1_result[i] == col1_result[i + 1] && col2_result[i] < col2_result[i + 1])) {
                Log::i("Sorting error at position {}: ({}, {}) should come after ({}, {})", 
                       i, col1_result[i], col2_result[i], col1_result[i + 1], col2_result[i + 1]);
                sortingCorrect = false;
            }
        }
        
        if (sortingCorrect) {
            Log::i("Multi-column sorting is CORRECT!");
        } else {
            Log::i("Multi-column sorting has ERRORS!");
        }
    }

    System::finalize();
}
