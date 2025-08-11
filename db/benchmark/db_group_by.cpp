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
