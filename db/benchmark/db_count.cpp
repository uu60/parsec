//
// Created by 杜建璋 on 25-8-30.
//

#include "secret/Secrets.h"
#include "utils/System.h"

#include "../include/basis/View.h"
#include "../include/operator/SelectSupport.h"
#include "../include/basis/Views.h"
#include "utils/Log.h"
#include "utils/StringUtils.h"

#include <string>
#include <vector>

#include "conf/DbConf.h"

int main(int argc, char *argv[]) {
    System::init(argc, argv);
    DbConf::init();

    // Fixed test data for count operation with filter
    // Test scenario: Count records where age > 25 and salary > 50000
    std::vector<int64_t> test_ids = {1, 2, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    std::vector<int64_t> test_ages = {20, 30, 35, 25, 35, 22, 40, 28, 45, 23, 50};
    std::vector<int64_t> test_salaries = {30000, 60000, 70000, 45000, 70000, 35000, 80000, 55000, 90000, 40000, 100000};
    
    // Expected result: Records that satisfy age > 25 AND salary > 50000
    // Record 2: age=30, salary=60000 ✓
    // Record 4: age=35, salary=70000 ✓  
    // Record 6: age=40, salary=80000 ✓
    // Record 7: age=28, salary=55000 ✓
    // Record 8: age=45, salary=90000 ✓
    // Record 10: age=50, salary=100000 ✓
    // Expected count: 6
    
    std::vector<int64_t> id_shares, age_shares, salary_shares;
    
    if (Comm::rank() == 2) {
        id_shares = test_ids;
        age_shares = test_ages;
        salary_shares = test_salaries;
        Log::i("Original test data:");
        Log::i("  IDs:      {}", StringUtils::vecToString(test_ids));
        Log::i("  Ages:     {}", StringUtils::vecToString(test_ages));
        Log::i("  Salaries: {}", StringUtils::vecToString(test_salaries));
        Log::i("Filter conditions: age > 25 AND salary > 50000");
        Log::i("Expected count: 6 records");
    }

    // Convert to secret shares
    id_shares = Secrets::boolShare(id_shares, 2, 64, System::nextTask());
    age_shares = Secrets::boolShare(age_shares, 2, 64, System::nextTask());
    salary_shares = Secrets::boolShare(salary_shares, 2, 64, System::nextTask());

    View v;
    
    if (Comm::isServer()) {
        // Create a view with the test data
        std::string name = "employee_table";
        std::vector<std::string> fieldNames = {"id", "age", "salary"};
        std::vector<int> fieldWidths = {64, 64, 64};

        Table t(name, fieldNames, fieldWidths, "");

        // Insert test data
        for (size_t i = 0; i < id_shares.size(); i++) {
            std::vector<int64_t> row = {id_shares[i], age_shares[i], salary_shares[i]};
            t.insert(row);
        }

        v = Views::selectAll(t);
        
        Log::i("Starting filter and count operation...");
        auto start = System::currentTimeMillis();
        
        // Apply filter: age > 25 AND salary > 50000
        std::vector<std::string> filterFields = {"age", "salary"};
        std::vector<View::ComparatorType> comparators = {View::ComparatorType::GREATER, View::ComparatorType::GREATER};
        std::vector<int64_t> filterValues = {
            Comm::rank() == 1 ? 0ll : 25ll,    // age > 25
            Comm::rank() == 1 ? 0ll : 50000ll  // salary > 50000
        };
        
        v.filterAndConditions(filterFields, comparators, filterValues, System::nextTask());

        
        auto filterEnd = System::currentTimeMillis();
        Log::i("Filter operation completed in {}ms", filterEnd - start);
        
        // Perform group by operation (required before count)
        // Since we want total count, we group by a constant field
        std::vector<std::string> groupFields = {"id"}; // Empty for total count
        std::vector<int64_t> groupBoundaries = v.groupBy(groupFields, System::nextTask());
        
        auto groupEnd = System::currentTimeMillis();
        Log::i("Group by operation completed in {}ms", groupEnd - filterEnd);
        
        // Perform count operation
        std::string countAlias = View::COUNT_COL_NAME;
        v.count(groupFields, groupBoundaries, countAlias, System::nextTask());
        
        auto countEnd = System::currentTimeMillis();
        Log::i("Count operation completed in {}ms", countEnd - groupEnd);
        Log::i("Total operation time: {}ms", countEnd - start);
    }

    Views::revealAndPrint(v);
    //
    // // Reconstruct and verify results
    // if (Comm::isServer()) {
    //     // Find the count column index
    //     int countColIndex = -1;
    //     for (size_t i = 0; i < v._fieldNames.size(); i++) {
    //         if (v._fieldNames[i] == View::COUNT_COL_NAME) {
    //             countColIndex = i;
    //             break;
    //         }
    //     }
    //
    //     if (countColIndex != -1) {
    //         auto countShares = v._dataCols[countColIndex];
    //         auto reconstructedCounts = Secrets::boolReconstruct(countShares, 2, 64, System::nextTask());
    //
    //         if (Comm::rank() == 2) {
    //             Log::i("Reconstructed count results: {}", StringUtils::vecToString(reconstructedCounts));
    //
    //             // Verify the result
    //             if (!reconstructedCounts.empty()) {
    //                 int64_t actualCount = reconstructedCounts[0]; // First (and should be only) result
    //                 int64_t expectedCount = 6;
    //
    //                 Log::i("Count verification:");
    //                 Log::i("  Expected count: {}", expectedCount);
    //                 Log::i("  Actual count:   {}", actualCount);
    //
    //                 if (actualCount == expectedCount) {
    //                     Log::i("✅ Count test PASSED!");
    //                 } else {
    //                     Log::e("❌ Count test FAILED - count mismatch");
    //                 }
    //             } else {
    //                 Log::e("❌ Count test FAILED - no results returned");
    //             }
    //         }
    //     } else {
    //         if (Comm::rank() == 2) {
    //             Log::e("❌ Count test FAILED - count column not found");
    //         }
    //     }
    //
    //     // Also verify the filtered data for debugging
    //     Log::i("Verifying filtered data...");
    //     for (size_t colIdx = 0; colIdx < v._fieldNames.size(); colIdx++) {
    //         if (v._fieldNames[colIdx] != View::VALID_COL_NAME &&
    //             v._fieldNames[colIdx] != View::PADDING_COL_NAME &&
    //             v._fieldNames[colIdx] != View::COUNT_COL_NAME) {
    //
    //             auto colShares = v._dataCols[colIdx];
    //             auto reconstructedCol = Secrets::boolReconstruct(colShares, 2, 64, System::nextTask());
    //
    //             if (Comm::rank() == 2) {
    //                 Log::i("Column '{}': {}", v._fieldNames[colIdx], StringUtils::vecToString(reconstructedCol));
    //             }
    //         }
    //     }
    // }

    System::finalize();
    return 0;
}
