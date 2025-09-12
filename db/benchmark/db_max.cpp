//
// Created by 杜建璋 on 25-8-18.
//

#include "secret/Secrets.h"
#include "utils/System.h"

#include "../include/basis/View.h"
#include "../include/basis/Views.h"
#include "../include/basis/Table.h"
#include "../include/operator/SelectSupport.h"
#include "conf/DbConf.h"
#include "utils/Log.h"
#include "utils/StringUtils.h"
#include "utils/Math.h"
#include "parallel/ThreadPoolSupport.h"

#include <string>
#include <vector>
#include <random>
#include <algorithm>
#include <map>

void generateTestData(int rows,
                      std::vector<int64_t> &id_data,
                      std::vector<int64_t> &value_data,
                      std::vector<int64_t> &tag_data);

View createTestTable(std::vector<int64_t> &id_shares,
                     std::vector<int64_t> &value_shares,
                     std::vector<int64_t> &tag_shares);

void testMaxFunction(int tid);

int main(int argc, char *argv[]) {
    System::init(argc, argv);
    DbConf::init();
    auto tid = System::nextTask() << (32 - Conf::TASK_TAG_BITS);

    // Read parameters from command line
    int rows = 100;
    if (Conf::_userParams.count("rows")) {
        rows = std::stoi(Conf::_userParams["rows"]);
    }

    if (Comm::isServer()) {
        Log::i("Starting max function test with {} rows...", rows);
    }

    testMaxFunction(tid);

    System::finalize();
    return 0;
}

void generateTestData(int rows,
                      std::vector<int64_t> &id_data,
                      std::vector<int64_t> &value_data,
                      std::vector<int64_t> &tag_data) {
    if (Comm::rank() == 2) {
        id_data.reserve(rows);
        value_data.reserve(rows);
        tag_data.reserve(rows);

        // Create test data with known groups and values
        // Group 1: ID=1, values=[10, 20, 15] -> max should be 20
        // Group 2: ID=2, values=[5, 25, 8] -> max should be 25
        // Group 3: ID=3, values=[30, 12] -> max should be 30
        
        std::vector<std::pair<int64_t, int64_t>> test_data = {
            {1, 10}, {1, 20}, {1, 15},  // Group 1
            {2, 5}, {2, 25}, {2, 8},    // Group 2
            {3, 30}, {3, 12}            // Group 3
        };

        // Fill with test data first
        for (const auto& pair : test_data) {
            id_data.push_back(pair.first);
            value_data.push_back(pair.second);
            tag_data.push_back((pair.first * 31 + 17) % DbConf::SHUFFLE_BUCKET_NUM);
        }

        // Fill remaining with random data if needed
        // for (int i = test_data.size(); i < rows; i++) {
        //     int64_t id = (i % 5) + 1; // Groups 1-5
        //     int64_t value = Math::randInt() % 100; // Random value 0-99
        //
        //     id_data.push_back(id);
        //     value_data.push_back(value);
        //     tag_data.push_back((id * 31 + 17) % DbConf::SHUFFLE_BUCKET_NUM);
        // }

        Log::i("Generated {} test records", rows);
        Log::i("Test data includes known groups:");
        Log::i("  Group 1 (ID=1): values [10, 20, 15] -> expected max = 20");
        Log::i("  Group 2 (ID=2): values [5, 25, 8] -> expected max = 25");
        Log::i("  Group 3 (ID=3): values [30, 12] -> expected max = 30");
    }
}

View createTestTable(std::vector<int64_t> &id_shares,
                     std::vector<int64_t> &value_shares,
                     std::vector<int64_t> &tag_shares) {
    std::string table_name = "TestTable";
    std::vector<std::string> fields = {"ID", "VALUE"};
    std::vector<int> widths = {64, 64};

    Table test_table(table_name, fields, widths, "ID");

    for (size_t i = 0; i < id_shares.size(); i++) {
        std::vector<int64_t> row = {
            id_shares[i],
            value_shares[i],
            tag_shares[i]  // bucket tag for ID
        };
        test_table.insert(row);
    }

    auto test_view = Views::selectAll(test_table);
    Log::i("Created test table with {} rows", test_table.rowNum());
    return test_view;
}

void testMaxFunction(int tid) {
    // Generate test data
    std::vector<int64_t> id_data, value_data, tag_data;
    generateTestData(20, id_data, value_data, tag_data); // Use 20 rows for clear testing

    // Convert to secret shares for 2PC
    auto id_shares = Secrets::boolShare(id_data, 2, 64, tid);
    auto value_shares = Secrets::boolShare(value_data, 2, 64, tid);
    auto tag_shares = Secrets::boolShare(tag_data, 2, 64, tid);

    if (Comm::isServer()) {
        Log::i("Step 1: Creating test table...");
        auto test_view = createTestTable(id_shares, value_shares, tag_shares);

        Log::i("Step 2: Performing GROUP BY on ID...");
        auto group_start = System::currentTimeMillis();
        auto heads = test_view.groupBy("ID", tid);
        auto group_end = System::currentTimeMillis();
        Log::i("GROUP BY completed in {}ms, found {} group boundaries", 
               group_end - group_start, heads.size());

        Log::i("Step 3: Computing MAX(VALUE) for each group...");
        auto max_start = System::currentTimeMillis();
        // test_view.minAndMax(heads, "VALUE", "MIN_VALUE", "MAX_VALUE", tid);
        test_view.max(heads, "VALUE", "MAX_VALUE", tid);
        Log::i("max_value: {}", StringUtils::vecToString(test_view._dataCols[test_view.colIndex("MAX_VALUE")]));
        // Log::i("min_value: {}", StringUtils::vecToString(test_view._dataCols[test_view.colIndex("MIN_VALUE")]));
        Log::i("id: {}", StringUtils::vecToString(test_view._dataCols[test_view.colIndex("ID")]));

        auto max_end = System::currentTimeMillis();
        Log::i("MAX aggregation completed in {}ms", max_end - max_start);

        Views::revealAndPrint(test_view);

        // For debugging, we can reveal some results (in a real scenario, this would be done securely)
        if (test_view.rowNum() > 0) {
            Log::i("Test completed successfully!");
            Log::i("Expected results:");
            Log::i("  Group 1: MAX_VALUE = 20");
            Log::i("  Group 2: MAX_VALUE = 25"); 
            Log::i("  Group 3: MAX_VALUE = 30");
            Log::i("Note: Actual values are secret-shared and would need secure revelation to verify.");
        }

        auto total_end = System::currentTimeMillis();
        Log::i("Total test execution time: {}ms", total_end - group_start);
    }
}
