//
// Created by 杜建璋 on 25-8-2.
//

#include "secret/Secrets.h"
#include "utils/System.h"

#include "../include/basis/View.h"
#include "../include/operator/SelectSupport.h"
#include "basis/Views.h"
#include "utils/Log.h"
#include "utils/StringUtils.h"

#include <string>
#include <vector>
#include <set>

int main(int argc, char *argv[]) {
    System::init(argc, argv);

    int num = 10;
    int col = 2;

    if (Conf::_userParams.count("num")) {
        num = std::stoi(Conf::_userParams["num"]);
    }

    if (Conf::_userParams.count("col")) {
        col = std::stoi(Conf::_userParams["col"]);
    }

    // Create test data with known duplicates for verification
    std::vector<std::vector<int64_t>> testData(col);
    
    if (Comm::rank() == 2) {
        Log::i("Creating test data with {} rows and {} columns", num, col);
        
        // Create specific test patterns with known duplicates
        if (col == 1) {
            // Single column: [1, 2, 1, 3, 2, 1, 4, 3] -> should become [1, 2, 3, 4]
            std::vector<int64_t> pattern = {1, 2, 1, 3, 2, 1, 4, 3};
            for (int i = 0; i < num; i++) {
                testData[0].push_back(pattern[i % pattern.size()]);
            }
        } else {
            // Multi-column: create combinations that will have duplicates
            // (1,1), (2,1), (1,2), (1,1), (2,2), (1,2), (3,1), (2,1) 
            // Should become: (1,1), (1,2), (2,1), (2,2), (3,1)
            std::vector<std::pair<int64_t, int64_t>> patterns = {
                {1,1}, {2,1}, {1,2}, {1,1}, {2,2}, {1,2}, {3,1}, {2,1}
            };
            for (int i = 0; i < num; i++) {
                auto& pattern = patterns[i % patterns.size()];
                testData[0].push_back(pattern.first);
                if (col > 1) testData[1].push_back(pattern.second);
                // For additional columns, use simple incremental pattern
                for (int c = 2; c < col; c++) {
                    testData[c].push_back((i % 3) + 1);
                }
            }
        }
        
        Log::i("Original data before sharing:");
        for (int c = 0; c < col; c++) {
            Log::i("Column {}: {}", c, StringUtils::vecString(testData[c]));
        }
    } else {
        // Initialize empty data for other ranks
        for (int c = 0; c < col; c++) {
            testData[c].resize(num, 0);
        }
    }

    // Share the data for each column
    std::vector<std::vector<int64_t>> sharedData(col);
    for (int c = 0; c < col; c++) {
        sharedData[c] = Secrets::boolShare(testData[c], 2, 64, System::nextTask());
    }

    View v;
    if (Comm::isServer()) {
        std::string name = "distinct_test";
        std::vector<std::string> fn(col);
        for (int i = 0; i < col; i++) {
            fn[i] = "col" + std::to_string(i);
        }
        std::vector<int> ws(col, 64);

        Table t(name, fn, ws, "col0");
        
        // Insert data row by row
        for (int i = 0; i < num; i++) {
            std::vector<int64_t> row(col + 1); // +1 for key field
            for (int j = 0; j < col; j++) {
                row[j] = sharedData[j][i];
            }
            row[col] = sharedData[0][i]; // key field same as first column
            t.insert(row);
        }

        v = Views::selectAll(t);
        
        Log::i("Before distinct: {} rows", v.rowNum());
        
        // Calculate tag stride for distinct operation
        int tagStride = v.distinctTagStride();
        Log::i("Distinct operation will use {} tags", tagStride);

        auto start = System::currentTimeMillis();
        v.distinct(0);
        auto duration = System::currentTimeMillis() - start;
        
        Log::i("After distinct: {} rows", v.rowNum());
        Log::i("Distinct operation time: {}ms", duration);
    }

    // Reconstruct and display results once for both display and verification
    std::vector<std::vector<int64_t>> results(col);
    
    for (int c = 0; c < col; c++) {
        auto secrets = Comm::isServer() ? v._dataCols[c] : std::vector<int64_t>();
        results[c] = Secrets::boolReconstruct(secrets, 2, 64, System::nextTask());
    }

    if (Comm::rank() == 2) {
        Log::i("Distinct results:");
        for (int c = 0; c < col; c++) {
            Log::i("Column {}: {}", c, StringUtils::vecString(results[c]));
        }
        
        // Display results in row format for easier verification
        Log::i("Result verification:");
        if (!results.empty() && !results[0].empty()) {
            for (size_t i = 0; i < results[0].size(); i++) {
                if (col == 1) {
                    Log::i("Row {}: ({})", i, results[0][i]);
                } else if (col == 2) {
                    Log::i("Row {}: ({}, {})", i, results[0][i], results[1][i]);
                } else {
                    std::string rowStr = "Row " + std::to_string(i) + ": (";
                    for (int c = 0; c < col; c++) {
                        if (c > 0) rowStr += ", ";
                        rowStr += std::to_string(results[c][i]);
                    }
                    rowStr += ")";
                    Log::i("{}", rowStr);
                }
            }
            
            // Verify distinctness
            bool isDistinct = true;
            std::set<std::vector<int64_t>> uniqueRows;
            
            for (size_t i = 0; i < results[0].size(); i++) {
                std::vector<int64_t> row(col);
                for (int c = 0; c < col; c++) {
                    row[c] = results[c][i];
                }
                
                if (uniqueRows.find(row) != uniqueRows.end()) {
                    Log::i("DUPLICATE FOUND: Row {} is not unique!", i);
                    isDistinct = false;
                } else {
                    uniqueRows.insert(row);
                }
            }
            
            if (isDistinct) {
                Log::i("DISTINCT operation is CORRECT - all rows are unique!");
            } else {
                Log::i("DISTINCT operation has ERRORS - duplicates found!");
            }
            
            // Additional verification for expected results
            if (col == 1 && num >= 8) {
                // Expected: [1, 2, 3, 4] (from pattern [1, 2, 1, 3, 2, 1, 4, 3])
                std::set<int64_t> expected = {1, 2, 3, 4};
                std::set<int64_t> actual;
                for (auto val : results[0]) {
                    actual.insert(val);
                }
                if (actual == expected) {
                    Log::i("Expected values verification: PASSED");
                } else {
                    Log::i("Expected values verification: FAILED");
                }
            } else if (col == 2 && num >= 8) {
                // Expected: (1,1), (1,2), (2,1), (2,2), (3,1)
                std::set<std::pair<int64_t, int64_t>> expected = {
                    {1,1}, {1,2}, {2,1}, {2,2}, {3,1}
                };
                std::set<std::pair<int64_t, int64_t>> actual;
                for (size_t i = 0; i < results[0].size(); i++) {
                    actual.insert({results[0][i], results[1][i]});
                }
                if (actual == expected) {
                    Log::i("Expected values verification: PASSED");
                } else {
                    Log::i("Expected values verification: FAILED");
                }
            }
        }
    }

    System::finalize();
    return 0;
}
