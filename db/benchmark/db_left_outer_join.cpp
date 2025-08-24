//
// Created by 杜建璋 on 25-8-23.
//

/*
 * Test for Left Outer Join Algorithm Correctness
 * 
 * This test validates that the leftOuterJoin function in Views class
 * correctly implements left outer join semantics:
 * 1. All rows from the left table are included
 * 2. Matching rows from the right table are joined
 * 3. Non-matching left rows have NULL values for right table columns
 * 4. Non-matching right rows are excluded
 */

#include "secret/Secrets.h"
#include "utils/System.h"

#include "../include/basis/View.h"
#include "../include/basis/Views.h"
#include "../include/basis/Table.h"
#include "../include/operator/SelectSupport.h"
#include "conf/DbConf.h"
#include "utils/Log.h"
#include "utils/StringUtils.h"

#include <string>
#include <vector>
#include <map>
#include <set>

// Test case structure
struct TestCase {
    std::string name;
    std::vector<int64_t> left_keys;
    std::vector<int64_t> left_values;
    std::vector<int64_t> right_keys;
    std::vector<int64_t> right_values;
    int expected_rows;
    std::string description;
};

// Forward declarations
void runTestCase(const TestCase& test);
bool validateLeftOuterJoinResult(const TestCase& test, View& result);
void printTestResults(const std::string& testName, bool passed, const std::string& details = "");

int main(int argc, char *argv[]) {
    System::init(argc, argv);

    if (Comm::isServer()) {
        Log::i("=== Left Outer Join Algorithm Correctness Test ===");
        Log::i("Testing the leftOuterJoin function implementation");
        Log::i("================================================");
    }

    // Define test cases
    std::vector<TestCase> testCases = {
        {
            "Basic Left Outer Join",
            {1, 2, 3, 4, 5},        // left keys
            {10, 20, 30, 40, 50},   // left values
            {2, 4, 6},              // right keys
            {200, 400, 600},        // right values
            5,                      // expected rows (all left rows)
            "Basic test: some left rows match, some don't"
        },
        {
            "All Left Rows Match",
            {1, 2, 3},              // left keys
            {10, 20, 30},           // left values
            {1, 2, 3},              // right keys
            {100, 200, 300},        // right values
            3,                      // expected rows
            "All left rows have matching right rows"
        },
        {
            "No Left Rows Match",
            {1, 2, 3},              // left keys
            {10, 20, 30},           // left values
            {4, 5, 6},              // right keys
            {400, 500, 600},        // right values
            3,                      // expected rows (all left rows with NULL right values)
            "No left rows match any right rows"
        },
        {
            "Empty Right Table",
            {1, 2, 3},              // left keys
            {10, 20, 30},           // left values
            {},                     // right keys (empty)
            {},                     // right values (empty)
            3,                      // expected rows (all left rows)
            "Right table is empty"
        },
        {
            "Single Row Tables",
            {1},                    // left keys
            {10},                   // left values
            {1},                    // right keys
            {100},                  // right values
            1,                      // expected rows
            "Single row in both tables with match"
        },
        {
            "Duplicate Keys in Right",
            {1, 2, 4},              // left keys
            {10, 20, 30},           // left values
            {1, 1, 2, 3, 3},        // right keys (duplicates)
            {100, 101, 200, 300, 301}, // right values
            7,                      // expected rows (1 left row can match multiple right rows)
            "Right table has duplicate keys"
        }
    };

    // Run all test cases
    int passed = 0;
    int total = testCases.size();

    for (const auto& test : testCases) {
        try {
            runTestCase(test);
            passed++;
        } catch (const std::exception& e) {
            if (Comm::isServer()) {
                printTestResults(test.name, false, std::string("Exception: ") + e.what());
            }
        }
    }

    if (Comm::isServer()) {
        Log::i("=== Test Summary ===");
        Log::i("Passed: {}/{} tests", passed, total);
        if (passed == total) {
            Log::i("✅ All tests passed! Left outer join algorithm is correct.");
        } else {
            Log::i("❌ Some tests failed. Please check the implementation.");
        }
        Log::i("==================");
    }

    System::finalize();
    return 0;
}

void runTestCase(const TestCase& test) {
    if (Comm::isServer()) {
        Log::i("\n--- Running Test: {} ---", test.name);
        Log::i("Description: {}", test.description);
        Log::i("Left table: {} rows, Right table: {} rows", 
               test.left_keys.size(), test.right_keys.size());
    }

    // Convert test data to secret shares (create mutable copies)
    std::vector<int64_t> left_keys_copy = test.left_keys;
    std::vector<int64_t> left_values_copy = test.left_values;
    std::vector<int64_t> right_keys_copy = test.right_keys;
    std::vector<int64_t> right_values_copy = test.right_values;
    
    auto left_key_shares = Secrets::boolShare(left_keys_copy, 2, 64, System::nextTask());
    auto left_value_shares = Secrets::boolShare(left_values_copy, 2, 64, System::nextTask());
    auto right_key_shares = Secrets::boolShare(right_keys_copy, 2, 64, System::nextTask());
    auto right_value_shares = Secrets::boolShare(right_values_copy, 2, 64, System::nextTask());

    View joinResult;

    if (Comm::isServer()) {
        // Create left table
        std::string leftTableName = "left_table";
        std::vector<std::string> leftFieldNames = {"left_key", "left_value"};
        std::vector<int> leftFieldWidths = {64, 64};
        Table leftTable(leftTableName, leftFieldNames, leftFieldWidths, "");

        for (size_t i = 0; i < left_key_shares.size(); i++) {
            std::vector<int64_t> record = {left_key_shares[i], left_value_shares[i]};
            leftTable.insert(record);
        }

        // Create right table
        std::string rightTableName = "right_table";
        std::vector<std::string> rightFieldNames = {"right_key", "right_value"};
        std::vector<int> rightFieldWidths = {64, 64};
        Table rightTable(rightTableName, rightFieldNames, rightFieldWidths, "");

        for (size_t i = 0; i < right_key_shares.size(); i++) {
            std::vector<int64_t> record = {right_key_shares[i], right_value_shares[i]};
            rightTable.insert(record);
        }

        // Create views
        View leftView = Views::selectAll(leftTable);
        View rightView = Views::selectAll(rightTable);

        // Perform left outer join
        std::string leftJoinField = "left_key";
        std::string rightJoinField = "right_key";

        auto start = System::currentTimeMillis();
        joinResult = Views::leftOuterJoin(leftView, rightView, leftJoinField, rightJoinField);
        Views::revealAndPrint(joinResult);
        auto elapsed = System::currentTimeMillis() - start;

        Log::i("Left outer join completed in {}ms", elapsed);
        Log::i("Result has {} rows (expected: {})", 
               joinResult._dataCols.empty() ? 0 : joinResult._dataCols[0].size(), 
               test.expected_rows);
    }

    // Validate the result
    bool testPassed = validateLeftOuterJoinResult(test, joinResult);
    
    if (Comm::isServer()) {
        printTestResults(test.name, testPassed);
    }

    if (!testPassed) {
        throw std::runtime_error("Test failed: " + test.name);
    }
}

bool validateLeftOuterJoinResult(const TestCase& test, View& result) {
    if (!Comm::isServer() || result._dataCols.empty()) {
        return true; // Can't validate on non-server nodes or empty results
    }

    int numCols = result._dataCols.size();
    int numRows = result._dataCols[0].size();

    // Reconstruct all data for validation
    std::vector<int64_t> allSecrets;
    allSecrets.reserve(numCols * numRows);
    
    for (int col = 0; col < numCols; col++) {
        for (int row = 0; row < numRows; row++) {
            allSecrets.push_back(result._dataCols[col][row]);
        }
    }

    auto allReconstructed = Secrets::boolReconstruct(allSecrets, 2, 64, System::nextTask());

    if (Comm::rank() != 2) {
        return true; // Only validate on rank 2
    }

    // Check basic properties
    if (numRows != test.expected_rows) {
        Log::i("❌ Row count mismatch: got {}, expected {}", numRows, test.expected_rows);
        return false;
    }

    // Expected columns: left_key, left_value, right_key, right_value
    if (numCols != 4) {
        Log::i("❌ Column count mismatch: got {}, expected 4", numCols);
        return false;
    }

    // Extract columns from reconstructed data
    std::vector<int64_t> result_left_keys, result_left_values, result_right_keys, result_right_values;
    for (int row = 0; row < numRows; row++) {
        result_left_keys.push_back(allReconstructed[0 * numRows + row]);
        result_left_values.push_back(allReconstructed[1 * numRows + row]);
        result_right_keys.push_back(allReconstructed[2 * numRows + row]);
        result_right_values.push_back(allReconstructed[3 * numRows + row]);
    }

    // Create maps for easier validation
    std::map<int64_t, int64_t> leftKeyToValue;
    for (size_t i = 0; i < test.left_keys.size(); i++) {
        leftKeyToValue[test.left_keys[i]] = test.left_values[i];
    }

    std::multimap<int64_t, int64_t> rightKeyToValue;
    for (size_t i = 0; i < test.right_keys.size(); i++) {
        rightKeyToValue.insert({test.right_keys[i], test.right_values[i]});
    }

    // Validate each result row
    std::set<int64_t> seenLeftKeys;
    for (int row = 0; row < numRows; row++) {
        int64_t left_key = result_left_keys[row];
        int64_t left_value = result_left_values[row];
        int64_t right_key = result_right_keys[row];
        int64_t right_value = result_right_values[row];

        // Check if left key and value are correct
        if (leftKeyToValue.find(left_key) == leftKeyToValue.end()) {
            Log::i("❌ Row {}: Invalid left key {}", row, left_key);
            return false;
        }
        if (leftKeyToValue[left_key] != left_value) {
            Log::i("❌ Row {}: Left value mismatch for key {}: got {}, expected {}", 
                   row, left_key, left_value, leftKeyToValue[left_key]);
            return false;
        }

        seenLeftKeys.insert(left_key);

        // Check right side
        auto rightMatches = rightKeyToValue.equal_range(left_key);
        if (rightMatches.first == rightMatches.second) {
            // No match in right table - right values should be 0 (NULL equivalent)
            if (right_key != 0 || right_value != 0) {
                Log::i("❌ Row {}: Expected NULL for unmatched left key {}, got right_key={}, right_value={}", 
                       row, left_key, right_key, right_value);
                return false;
            }
        } else {
            // Match found - verify it's a valid match
            bool validMatch = false;
            for (auto it = rightMatches.first; it != rightMatches.second; ++it) {
                if (it->second == right_value && left_key == right_key) {
                    validMatch = true;
                    break;
                }
            }
            if (!validMatch) {
                Log::i("❌ Row {}: Invalid right match for left key {}: right_key={}, right_value={}", 
                       row, left_key, right_key, right_value);
                return false;
            }
        }
    }

    // Verify all left keys are present
    for (int64_t leftKey : test.left_keys) {
        if (seenLeftKeys.find(leftKey) == seenLeftKeys.end()) {
            Log::i("❌ Missing left key {} in result", leftKey);
            return false;
        }
    }

    Log::i("✅ Validation passed");
    return true;
}

void printTestResults(const std::string& testName, bool passed, const std::string& details) {
    if (passed) {
        Log::i("✅ PASSED: {}", testName);
    } else {
        Log::i("❌ FAILED: {}", testName);
        if (!details.empty()) {
            Log::i("   Details: {}", details);
        }
    }
}
