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

    // Test data: group_keys = [0, 1, 1, 2, 2, 2, 3], values = [10, 20, 30, 40, 50, 60, 70]
    // Expected groups: 4 groups with IDs [1, 2, 2, 3, 3, 3, 4]
    std::vector<int64_t> test_keys = {0, 1, 1, 2, 2, 2, 3};
    std::vector<int64_t> test_values = {10, 20, 30, 40, 50, 60, 70};
    std::vector<int64_t> key_shares, value_shares;
    
    if (Comm::rank() == 2) {
        key_shares = test_keys;
        value_shares = test_values;
        Log::i("Original data:");
        Log::i("  Keys:   {}", StringUtils::vecString(test_keys));
        Log::i("  Values: {}", StringUtils::vecString(test_values));
        Log::i("Expected: 4 groups after sorting and grouping");
    }

    // Convert to secret shares
    key_shares = Secrets::boolShare(key_shares, 2, 64, System::nextTask());
    value_shares = Secrets::boolShare(value_shares, 2, 64, System::nextTask());

    View v;
    std::vector<int64_t> group_result;
    
    if (Comm::isServer()) {
        // Create a view with the test data
        std::string name = "test_table";
        std::vector<std::string> fieldNames = {"group_key", "value"};
        std::vector<int> fieldWidths = {64, 64};

        Table t(name, fieldNames, fieldWidths, "");

        // Insert test data
        for (size_t i = 0; i < key_shares.size(); i++) {
            std::vector<int64_t> row = {key_shares[i], value_shares[i]};
            t.insert(row);
        }

        v = Views::selectAll(t);
        
        Log::i("Starting group by operation...");
        auto start = System::currentTimeMillis();
        
        // Perform group by operation
        group_result = v.groupBy("group_key", System::nextTask());
        
        auto end = System::currentTimeMillis();
        Log::i("Group by operation completed in {}ms", end - start);
        Log::i("groups: {}", StringUtils::vecString(group_result));
    }

    System::finalize();
    return 0;
}

inline void test_multi_column_group_by() {
    int testSize = 20;

    if (Comm::isClient()) {
        Log::i("Testing Multi-Column Group By with {} rows", testSize);
    }

    auto t = System::nextTask();

    // Create test data with known grouping patterns
    std::vector<int64_t> col1_data, col2_data, col3_data;

    if (Comm::isClient()) {
        // Create test data with predictable groups
        // Group 1: (1, 10, 100) - 3 rows
        // Group 2: (1, 10, 200) - 2 rows
        // Group 3: (1, 20, 100) - 2 rows
        // Group 4: (2, 10, 100) - 3 rows
        // Group 5: (2, 20, 200) - 2 rows
        // Remaining rows: unique groups

        std::vector<std::tuple<int64_t, int64_t, int64_t>> testData = {
            // Group 1: (1, 10, 100) - 3 rows
            {1, 10, 100}, {1, 10, 100}, {1, 10, 100},
            // Group 2: (1, 10, 200) - 2 rows
            {1, 10, 200}, {1, 10, 200},
            // Group 3: (1, 20, 100) - 2 rows
            {1, 20, 100}, {1, 20, 100},
            // Group 4: (2, 10, 100) - 3 rows
            {2, 10, 100}, {2, 10, 100}, {2, 10, 100},
            // Group 5: (2, 20, 200) - 2 rows
            {2, 20, 200}, {2, 20, 200},
            // Unique groups
            {3, 30, 300}, {4, 40, 400}, {5, 50, 500}, {6, 60, 600},
            {7, 70, 700}, {8, 80, 800}, {9, 90, 900}, {10, 100, 1000}
        };

        for (const auto& row : testData) {
            col1_data.push_back(std::get<0>(row));
            col2_data.push_back(std::get<1>(row));
            col3_data.push_back(std::get<2>(row));
        }

        Log::i("Test data created:");
        Log::i("Column 1: {}", StringUtils::vecString(col1_data));
        Log::i("Column 2: {}", StringUtils::vecString(col2_data));
        Log::i("Column 3: {}", StringUtils::vecString(col3_data));
    }

    // Convert to secret shares
    auto col1_shares = Secrets::boolShare(col1_data, 2, 64, t);
    auto col2_shares = Secrets::boolShare(col2_data, 2, 64, t);
    auto col3_shares = Secrets::boolShare(col3_data, 2, 64, t);

    std::vector<int64_t> groupBoundaries;
    if (Comm::isServer()) {
        // Create View for testing
        std::vector<std::string> fieldNames = {"col1", "col2", "col3"};
        std::vector<int> fieldWidths = {64, 64, 64};
        View testView(fieldNames, fieldWidths);

        // Insert data into view (need to include valid and padding columns)
        for (size_t i = 0; i < col1_shares.size(); i++) {
            std::vector<int64_t> row = {col1_shares[i], col2_shares[i], col3_shares[i], Comm::rank(), 0};
            testView.insert(row);
        }

        Log::i("Created test view with {} rows", testView.rowNum());

        auto start = System::currentTimeMillis();

        // Test single column group by
        Log::i("Testing single column group by on col1...");
        auto singleGroupBoundaries = testView.groupBy("col1", t);

        // Test multi-column group by
        Log::i("Testing multi-column group by on (col1, col2, col3)...");
        std::vector<std::string> groupFields = {"col1", "col2", "col3"};
        groupBoundaries = testView.groupBy(groupFields, t);

        auto end = System::currentTimeMillis();
        Log::i("Multi-column group by execution time: {}ms", end - start);

        Log::i("Single column group boundaries (secret shares): {}", StringUtils::vecString(singleGroupBoundaries));
        Log::i("Multi-column group boundaries (secret shares): {}", StringUtils::vecString(groupBoundaries));
    }

    // Reconstruct results for verification
    auto reconstructedBoundaries = Secrets::boolReconstruct(groupBoundaries, 2, 1, t);

    if (Comm::isClient()) {
        Log::i("Reconstructed group boundaries: {}", StringUtils::vecString(reconstructedBoundaries));

        // Verify results
        int groupCount = 0;
        std::vector<int> expectedBoundaries;

        // Expected group boundaries based on our test data:
        // Row 0: (1,10,100) - boundary (first row)
        // Row 1: (1,10,100) - no boundary (same as previous)
        // Row 2: (1,10,100) - no boundary (same as previous)
        // Row 3: (1,10,200) - boundary (different col3)
        // Row 4: (1,10,200) - no boundary (same as previous)
        // Row 5: (1,20,100) - boundary (different col2)
        // Row 6: (1,20,100) - no boundary (same as previous)
        // Row 7: (2,10,100) - boundary (different col1)
        // Row 8: (2,10,100) - no boundary (same as previous)
        // Row 9: (2,10,100) - no boundary (same as previous)
        // Row 10: (2,20,200) - boundary (different col2 and col3)
        // Row 11: (2,20,200) - no boundary (same as previous)
        // Rows 12-19: all unique, so all boundaries

        std::vector<int> expectedPattern = {1, 0, 0, 1, 0, 1, 0, 1, 0, 0, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1};

        int correctCount = 0;
        int wrongCount = 0;

        for (int i = 0; i < reconstructedBoundaries.size() && i < expectedPattern.size(); i++) {
            bool expected = (expectedPattern[i] == 1);
            bool actual = (reconstructedBoundaries[i] == 1);

            if (expected == actual) {
                correctCount++;
                if (actual) groupCount++;
                Log::i("Row {}: CORRECT - data: ({},{},{}), expected boundary: {}, actual: {}",
                       i, col1_data[i], col2_data[i], col3_data[i], expected, actual);
            } else {
                wrongCount++;
                Log::e("Row {}: WRONG - data: ({},{},{}), expected boundary: {}, actual: {}",
                       i, col1_data[i], col2_data[i], col3_data[i], expected, actual);
            }
        }

        Log::i("Multi-Column Group By Test Summary:");
        Log::i("- {} correct, {} wrong out of {} tests", correctCount, wrongCount, reconstructedBoundaries.size());
        Log::i("- Total groups detected: {}", groupCount);
        Log::i("- Expected groups: 13 (based on test data pattern)");

        if (wrongCount == 0 && groupCount == 13) {
            Log::i("✅ Multi-Column Group By test PASSED!");
        } else {
            Log::e("❌ Multi-Column Group By test FAILED");
            if (wrongCount > 0) {
                Log::e("   - {} boundary detection errors", wrongCount);
            }
            if (groupCount != 13) {
                Log::e("   - Wrong group count: expected 13, got {}", groupCount);
            }
        }
    }
}
