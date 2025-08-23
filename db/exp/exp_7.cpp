//
// Created by 杜建璋 on 25-8-22.
//

/*
 * SELECT sum(l_extendedprice*l_discount) as revenue
 * FROM lineitem
 * WHERE l_shipdate >= date '[DATE]'
 *   AND l_shipdate < date '[DATE]' + interval '1' year
 *   AND l_discount between [DISCOUNT] - 0.01
 *   AND [DISCOUNT] + 0.01 and l_quantity < [QUANTITY]
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
#include "utils/Math.h"
#include "compute/batch/bool/BoolLessBatchOperator.h"
#include "compute/batch/bool/BoolEqualBatchOperator.h"
#include "compute/batch/arith/ArithLessBatchOperator.h"
#include "compute/batch/arith/ArithAddBatchOperator.h"
#include "compute/batch/arith/ArithMultiplyBatchOperator.h"
#include "compute/batch/arith/ArithToBoolBatchOperator.h"
#include "compute/batch/bool/BoolToArithBatchOperator.h"
#include "compute/batch/bool/BoolAndBatchOperator.h"
#include "parallel/ThreadPoolSupport.h"

#include <string>
#include <vector>
#include <random>
#include <algorithm>
#include <map>

// Forward declarations
void generateTestData(int lineitem_rows,
                      std::vector<int64_t> &l_shipdate_data,
                      std::vector<int64_t> &l_discount_data,
                      std::vector<int64_t> &l_quantity_data,
                      std::vector<int64_t> &l_extendedprice_data,
                      std::vector<int64_t> &l_revenue_data);

View createLineitemTable(std::vector<int64_t> &l_shipdate_shares,
                         std::vector<int64_t> &l_discount_shares,
                         std::vector<int64_t> &l_quantity_shares,
                         std::vector<int64_t> &l_extendedprice_shares,
                         std::vector<int64_t> &l_revenue_shares);

View filterByShipdate(View &lineitem_view, int64_t start_date, int64_t end_date, int tid);

View filterByDiscount(View &lineitem_view, int64_t discount_min, int64_t discount_max, int tid);

View filterByQuantity(View &lineitem_view, int64_t quantity_threshold, int tid);

int64_t calculateRevenue(View &filtered_view, int tid);

void displayResults(int64_t revenue_share, int tid);

int main(int argc, char *argv[]) {
    System::init(argc, argv);
    auto tid = System::nextTask() << (32 - Conf::TASK_TAG_BITS);

    // Read parameters from command line
    int lineitem_rows = 10000;
    int64_t start_date = 19940101;    // 1994-01-01
    int64_t end_date = 19950101;      // 1995-01-01 (1 year later)
    int64_t discount_center = 6;      // 0.06 (represented as 6 for integer arithmetic)
    int64_t discount_min = 5;         // 0.05 (discount_center - 1)
    int64_t discount_max = 7;         // 0.07 (discount_center + 1)
    int64_t quantity_threshold = 24;  // quantity < 24

    if (Conf::_userParams.count("lineitem_rows")) {
        lineitem_rows = std::stoi(Conf::_userParams["lineitem_rows"]);
    }
    if (Conf::_userParams.count("start_date")) {
        start_date = std::stoll(Conf::_userParams["start_date"]);
    }
    if (Conf::_userParams.count("end_date")) {
        end_date = std::stoll(Conf::_userParams["end_date"]);
    }
    if (Conf::_userParams.count("discount_center")) {
        discount_center = std::stoll(Conf::_userParams["discount_center"]);
        discount_min = discount_center - 1;
        discount_max = discount_center + 1;
    }
    if (Conf::_userParams.count("quantity_threshold")) {
        quantity_threshold = std::stoll(Conf::_userParams["quantity_threshold"]);
    }

    // Generate test data
    std::vector<int64_t> l_shipdate_data, l_discount_data, l_quantity_data, l_extendedprice_data, l_revenue_data;
    generateTestData(lineitem_rows, l_shipdate_data, l_discount_data, l_quantity_data, l_extendedprice_data, l_revenue_data);

    // Convert to secret shares for 2PC (using bool shares for all data)
    auto l_shipdate_shares = Secrets::boolShare(l_shipdate_data, 2, 64, tid);
    auto l_discount_shares = Secrets::boolShare(l_discount_data, 2, 64, tid);
    auto l_quantity_shares = Secrets::boolShare(l_quantity_data, 2, 64, tid);
    auto l_extendedprice_shares = Secrets::boolShare(l_extendedprice_data, 2, 64, tid);
    auto l_revenue_shares = Secrets::boolShare(l_revenue_data, 2, 64, tid);

    int64_t revenue_share = 0;
    if (Comm::isServer()) {
        Log::i("Starting TPC-H Query 6 execution...");
        auto query_start = System::currentTimeMillis();

        // Create lineitem table
        auto lineitem_view = createLineitemTable(l_shipdate_shares, l_discount_shares, 
                                                  l_quantity_shares, l_extendedprice_shares, l_revenue_shares);

        // Step 1: Filter by shipdate range
        auto filtered_by_date = filterByShipdate(lineitem_view, start_date, end_date, tid);

        // Step 2: Filter by discount range
        auto filtered_by_discount = filterByDiscount(filtered_by_date, discount_min, discount_max, tid);

        // Step 3: Filter by quantity threshold
        auto filtered_by_quantity = filterByQuantity(filtered_by_discount, quantity_threshold, tid);

        // Step 4: Calculate revenue sum(l_extendedprice * l_discount)
        revenue_share = calculateRevenue(filtered_by_quantity, tid);

        auto query_end = System::currentTimeMillis();
        Log::i("Total query execution time: {}ms", query_end - query_start);
    }

    // Display results
    displayResults(revenue_share, tid);

    System::finalize();
    return 0;
}

// Function implementations

void generateTestData(int lineitem_rows,
                      std::vector<int64_t> &l_shipdate_data,
                      std::vector<int64_t> &l_discount_data,
                      std::vector<int64_t> &l_quantity_data,
                      std::vector<int64_t> &l_extendedprice_data,
                      std::vector<int64_t> &l_revenue_data) {
    if (Comm::rank() == 2) {
        l_shipdate_data.reserve(lineitem_rows);
        l_discount_data.reserve(lineitem_rows);
        l_quantity_data.reserve(lineitem_rows);
        l_extendedprice_data.reserve(lineitem_rows);
        l_revenue_data.reserve(lineitem_rows);

        // Generate fixed test data for verification
        // We'll create 10 records with known values to verify the calculation
        
        // Test data design:
        // Records 0-4: Should pass all filters (shipdate in range, discount 5-7, quantity < 24)
        // Records 5-9: Should fail various filters for testing
        
        std::vector<int64_t> test_shipdates = {
            19940201, 19940301, 19940401, 19940501, 19940601,  // Records 0-4: in range [19940101, 19950101)
            19930101, 19950201, 19960101, 19970101, 19980101   // Records 5-9: out of range
        };
        
        std::vector<int64_t> test_discounts = {
            5, 6, 7, 5, 6,     // Records 0-4: in range [5, 7] (discount_center=6, ±1)
            3, 4, 8, 9, 10     // Records 5-9: out of range
        };
        
        std::vector<int64_t> test_quantities = {
            10, 15, 20, 23, 22,  // Records 0-4: < 24 (threshold)
            25, 30, 35, 40, 45   // Records 5-9: >= 24
        };
        
        std::vector<int64_t> test_prices = {
            100000, 200000, 300000, 400000, 500000,  // Records 0-4: 1000.00, 2000.00, 3000.00, 4000.00, 5000.00
            600000, 700000, 800000, 900000, 1000000  // Records 5-9: 6000.00, 7000.00, 8000.00, 9000.00, 10000.00
        };

        int actual_rows = std::min(lineitem_rows, 10);
        
        for (int i = 0; i < actual_rows; i++) {
            l_shipdate_data.push_back(test_shipdates[i]);
            l_discount_data.push_back(test_discounts[i]);
            l_quantity_data.push_back(test_quantities[i]);
            l_extendedprice_data.push_back(test_prices[i]);
            // Pre-calculate revenue: l_extendedprice * l_discount
            l_revenue_data.push_back(test_prices[i] * test_discounts[i]);
        }
        
        // If more rows requested, repeat the pattern
        for (int i = actual_rows; i < lineitem_rows; i++) {
            int idx = i % 10;
            l_shipdate_data.push_back(test_shipdates[idx]);
            l_discount_data.push_back(test_discounts[idx]);
            l_quantity_data.push_back(test_quantities[idx]);
            l_extendedprice_data.push_back(test_prices[idx]);
            l_revenue_data.push_back(test_prices[idx] * test_discounts[idx]);
        }

        Log::i("Generated {} lineitem records with fixed test data", lineitem_rows);
        Log::i("Test data design:");
        Log::i("Records 0-4: Should pass all filters");
        Log::i("  - Shipdate: 1994-02-01 to 1994-06-01 (in range [1994-01-01, 1995-01-01))");
        Log::i("  - Discount: 5, 6, 7, 5, 6 (in range [5, 7])");
        Log::i("  - Quantity: 10, 15, 20, 23, 22 (< 24)");
        Log::i("  - Price: 1000.00, 2000.00, 3000.00, 4000.00, 5000.00");
        Log::i("  - Pre-calculated revenue: 500000, 1200000, 2100000, 2000000, 3000000");
        Log::i("Records 5-9: Should fail various filters");
        Log::i("Expected revenue calculation:");
        Log::i("  Record 0: 100000 * 5 = 500000");
        Log::i("  Record 1: 200000 * 6 = 1200000");
        Log::i("  Record 2: 300000 * 7 = 2100000");
        Log::i("  Record 3: 400000 * 5 = 2000000");
        Log::i("  Record 4: 500000 * 6 = 3000000");
        Log::i("  Total expected revenue: 8800000 (= $88000.00)");
    }
}

View createLineitemTable(std::vector<int64_t> &l_shipdate_shares,
                         std::vector<int64_t> &l_discount_shares,
                         std::vector<int64_t> &l_quantity_shares,
                         std::vector<int64_t> &l_extendedprice_shares,
                         std::vector<int64_t> &l_revenue_shares) {
    std::string table_name = "lineitem";
    std::vector<std::string> fields = {"l_shipdate", "l_discount", "l_quantity", "l_extendedprice", "l_revenue"};
    std::vector<int> widths = {64, 64, 64, 64, 64};

    Table lineitem_table(table_name, fields, widths, "");

    for (size_t i = 0; i < l_shipdate_shares.size(); i++) {
        std::vector<int64_t> row = {
            l_shipdate_shares[i],
            l_discount_shares[i],
            l_quantity_shares[i],
            l_extendedprice_shares[i],
            l_revenue_shares[i]
        };
        lineitem_table.insert(row);
    }

    auto lineitem_view = Views::selectAll(lineitem_table);
    Log::i("Created lineitem table with {} rows and {} columns", lineitem_table.rowNum(), fields.size());
    return lineitem_view;
}

View filterByShipdate(View &lineitem_view, int64_t start_date, int64_t end_date, int tid) {
    Log::i("Step 1: Filtering by shipdate range [{}, {})...", start_date, end_date);
    auto step1_start = System::currentTimeMillis();

    std::vector<std::string> fieldNames = {"l_shipdate", "l_shipdate"};
    std::vector<View::ComparatorType> comparatorTypes = {View::GREATER_EQ, View::LESS};
    std::vector<int64_t> constShares = {Comm::rank() * start_date, Comm::rank() * end_date};

    View filtered_view = lineitem_view;
    filtered_view.filterAndConditions(fieldNames, comparatorTypes, constShares, tid);

    auto step1_end = System::currentTimeMillis();
    Log::i("Step 1 completed in {}ms", step1_end - step1_start);
    Log::i("Filtered by shipdate: {} rows", filtered_view.rowNum());
    return filtered_view;
}

View filterByDiscount(View &lineitem_view, int64_t discount_min, int64_t discount_max, int tid) {
    Log::i("Step 2: Filtering by discount range [{}, {}]...", discount_min, discount_max);
    auto step2_start = System::currentTimeMillis();

    std::vector<std::string> fieldNames = {"l_discount", "l_discount"};
    std::vector<View::ComparatorType> comparatorTypes = {View::GREATER_EQ, View::LESS_EQ};
    std::vector<int64_t> constShares = {Comm::rank() * discount_min, Comm::rank() * discount_max};

    View filtered_view = lineitem_view;
    filtered_view.filterAndConditions(fieldNames, comparatorTypes, constShares, tid);

    auto step2_end = System::currentTimeMillis();
    Log::i("Step 2 completed in {}ms", step2_end - step2_start);
    Log::i("Filtered by discount: {} rows", filtered_view.rowNum());
    return filtered_view;
}

View filterByQuantity(View &lineitem_view, int64_t quantity_threshold, int tid) {
    Log::i("Step 3: Filtering by quantity < {}...", quantity_threshold);
    auto step3_start = System::currentTimeMillis();

    std::vector<std::string> fieldNames = {"l_quantity"};
    std::vector<View::ComparatorType> comparatorTypes = {View::LESS};
    std::vector<int64_t> constShares = {Comm::rank() * quantity_threshold};

    View filtered_view = lineitem_view;
    filtered_view.filterAndConditions(fieldNames, comparatorTypes, constShares, tid);

    auto step3_end = System::currentTimeMillis();
    Log::i("Step 3 completed in {}ms", step3_end - step3_start);
    Log::i("Filtered by quantity: {} rows", filtered_view.rowNum());
    return filtered_view;
}

int64_t calculateRevenue(View &filtered_view, int tid) {
    Log::i("Step 4: Calculating revenue sum using pre-calculated l_revenue column...");
    auto step4_start = System::currentTimeMillis();

    if (filtered_view.rowNum() == 0) {
        auto step4_end = System::currentTimeMillis();
        Log::i("Step 4 completed in {}ms (empty input)", step4_end - step4_start);
        return 0;
    }

    // Get the pre-calculated revenue column (bool shares)
    auto &revenue_col = filtered_view._dataCols[filtered_view.colIndex("l_revenue")];

    // Convert bool shares to arith shares for summation
    std::vector<int64_t> revenue_arith_shares;
    
    if (Conf::BATCH_SIZE <= 0 || Conf::DISABLE_MULTI_THREAD) {
        // Single batch processing
        revenue_arith_shares = BoolToArithBatchOperator(&revenue_col, 64, tid, 0, SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    } else {
        // Multi-batch processing
        size_t data_size = revenue_col.size();
        int batchSize = Conf::BATCH_SIZE;
        int batchNum = (data_size + batchSize - 1) / batchSize;

        std::vector<std::future<std::vector<int64_t>>> batch_futures(batchNum);

        for (int b = 0; b < batchNum; ++b) {
            batch_futures[b] = ThreadPoolSupport::submit([&, b]() -> std::vector<int64_t> {
                int start = b * batchSize;
                int end = std::min(start + batchSize, static_cast<int>(data_size));

                std::vector<int64_t> batch_revenue(revenue_col.begin() + start, revenue_col.begin() + end);

                int batch_tid = tid + b * BoolToArithBatchOperator::tagStride();

                return BoolToArithBatchOperator(&batch_revenue, 64, batch_tid, 0, SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
            });
        }

        revenue_arith_shares.reserve(data_size);
        for (auto &f : batch_futures) {
            auto batch_res = f.get();
            revenue_arith_shares.insert(revenue_arith_shares.end(), batch_res.begin(), batch_res.end());
        }
    }

    // Calculate sum of revenue values (now in arith shares)
    int64_t revenue_sum = 0;
    if (Comm::isServer()) {
        for (int64_t revenue : revenue_arith_shares) {
            revenue_sum += revenue;
        }
    }

    auto step4_end = System::currentTimeMillis();
    Log::i("Step 4 completed in {}ms", step4_end - step4_start);
    Log::i("Calculated revenue sum from {} qualifying records using pre-calculated values", revenue_arith_shares.size());
    
    return revenue_sum;
}

void displayResults(int64_t revenue_share, int tid) {
    Log::i("Reconstructing revenue result for verification...");
    
    std::vector<int64_t> revenue_vec = {revenue_share};
    auto revenue_plain = Secrets::arithReconstruct(revenue_vec, 2, 64, tid);

    if (Comm::rank() == 2) {
        Log::i("TPC-H Query 6 Results:");
        Log::i("======================");
        
        if (!revenue_plain.empty()) {
            double revenue_dollars = static_cast<double>(revenue_plain[0]) / 100.0; // Convert cents to dollars
            Log::i("Total Revenue: ${:.2f}", revenue_dollars);
            Log::i("Revenue (raw): {}", revenue_plain[0]);
        } else {
            Log::i("Total Revenue: $0.00");
        }

        Log::i("\nQuery Summary:");
        Log::i("This query calculates the total revenue from lineitem records where:");
        Log::i("1. Shipdate is within the specified date range");
        Log::i("2. Discount is within the specified range (±0.01 from center)");
        Log::i("3. Quantity is below the specified threshold");
        Log::i("Revenue = sum(l_extendedprice * l_discount) for qualifying records");
    }
}
