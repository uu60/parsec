//
// Created by 杜建璋 on 2025/8/2.
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
#include <map>

int main(int argc, char *argv[]) {
    System::init(argc, argv);

    // Test data: [0, 1, 1, 2, 2, 2, 3] - should group as: 0->1, 1->2, 2->3, 3->1
    std::vector<int64_t> test_keys = {0, 1, 1, 2, 2, 2, 3};
    std::vector<int64_t> shares;
    
    if (Comm::rank() == 2) {
        shares = test_keys;
        Log::i("Original data: {}", StringUtils::vecString(test_keys));
        
        // Expected results for verification
        std::map<int64_t, int64_t> expected = {{0, 1}, {1, 2}, {2, 3}, {3, 1}};
        Log::i("Expected group by results:");
        for (const auto& pair : expected) {
            Log::i("  Key {} -> Count {}", pair.first, pair.second);
        }
    }

    // Convert to secret shares
    shares = Secrets::boolShare(shares, 2, 64, System::nextTask());

    View v;
    std::vector<std::pair<std::vector<int64_t>, int64_t>> group_results;
    
    if (Comm::isServer()) {
        // Create a view with the test data
        std::string name = "test_table";
        std::vector<std::string> fieldNames = {"group_key", "value"};
        std::vector<int> fieldWidths = {64, 64};

        Table t(name, fieldNames, fieldWidths, "group_key");
        
        // Insert test data
        for (size_t i = 0; i < shares.size(); i++) {
            std::vector<int64_t> row = {shares[i], shares[i]}; // Use same value for both columns
            t.insert(row);
        }

        v = Views::selectAll(t);
        
        Log::i("Starting group by operation...");
        auto start = System::currentTimeMillis();
        
        // Perform group by operation
        group_results = v.groupBy("group_key", System::nextTask());
        
        auto end = System::currentTimeMillis();
        Log::i("Group by operation completed in {}ms", end - start);
        Log::i("Number of groups found: {}", group_results.size());
    }

    // Reconstruct and verify results
    if (Comm::rank() == 2) {
        Log::i("Group by results:");
        Log::i("Number of results returned: {}", group_results.size());
        std::map<int64_t, int64_t> actual_results;
        
        for (size_t i = 0; i < group_results.size(); i++) {
            const auto& result = group_results[i];
            int64_t key = result.first[0];  // First element of key vector
            int64_t count = result.second;
            actual_results[key] = count;
            Log::i("  Result {}: Key {} -> Count {}", i, key, count);
        }
        
        // Verify correctness
        std::map<int64_t, int64_t> expected = {{0, 1}, {1, 2}, {2, 3}, {3, 1}};
        bool all_correct = true;
        
        Log::i("Verification:");
        for (const auto& expected_pair : expected) {
            int64_t expected_key = expected_pair.first;
            int64_t expected_count = expected_pair.second;
            
            if (actual_results.find(expected_key) != actual_results.end()) {
                int64_t actual_count = actual_results[expected_key];
                if (actual_count == expected_count) {
                    Log::i("  ✓ Key {}: Expected {}, Got {}", expected_key, expected_count, actual_count);
                } else {
                    Log::i("  ✗ Key {}: Expected {}, Got {}", expected_key, expected_count, actual_count);
                    all_correct = false;
                }
            } else {
                Log::i("  ✗ Key {}: Expected {}, Got MISSING", expected_key, expected_count);
                all_correct = false;
            }
        }
        
        // Check for unexpected keys
        for (const auto& actual_pair : actual_results) {
            if (expected.find(actual_pair.first) == expected.end()) {
                Log::i("  ✗ Unexpected key {}: Got count {}", actual_pair.first, actual_pair.second);
                all_correct = false;
            }
        }
        
        if (all_correct) {
            Log::i("🎉 All group by results are CORRECT!");
        } else {
            Log::i("❌ Some group by results are INCORRECT!");
        }
    }

    System::finalize();
    return 0;
}
