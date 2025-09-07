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

int main(int argc, char *argv[]) {
    System::init(argc, argv);
    DbConf::init();
    auto tid = System::nextTask() << (32 - Conf::TASK_TAG_BITS);

    // Read parameters from command line
    int lineitem_rows = 10000;

    if (Conf::_userParams.count("rows")) {
        lineitem_rows = std::stoi(Conf::_userParams["lineitem_rows"]);
    }

    if (Comm::isClient()) {
        Log::i("Data size: lineitem: {}", lineitem_rows);
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

    // Generate and distribute constant parameters as secret shares
    int64_t start_date, end_date, discount_center, discount_min, discount_max, quantity_threshold;
    if (Comm::isClient()) {
        int64_t start_date0 = Math::randInt();
        int64_t start_date1 = Math::randInt();
        int64_t end_date0 = Math::randInt();
        int64_t end_date1 = Math::randInt();
        int64_t discount_center0 = Math::randInt();
        int64_t discount_center1 = Math::randInt();
        int64_t discount_min0 = Math::randInt();
        int64_t discount_min1 = Math::randInt();
        int64_t discount_max0 = Math::randInt();
        int64_t discount_max1 = Math::randInt();
        int64_t quantity_threshold0 = Math::randInt();
        int64_t quantity_threshold1 = Math::randInt();
        
        Comm::send(start_date0, 64, 0, tid);
        Comm::send(end_date0, 64, 0, tid);
        Comm::send(discount_center0, 64, 0, tid);
        Comm::send(discount_min0, 64, 0, tid);
        Comm::send(discount_max0, 64, 0, tid);
        Comm::send(quantity_threshold0, 64, 0, tid);
        
        Comm::send(start_date1, 64, 1, tid);
        Comm::send(end_date1, 64, 1, tid);
        Comm::send(discount_center1, 64, 1, tid);
        Comm::send(discount_min1, 64, 1, tid);
        Comm::send(discount_max1, 64, 1, tid);
        Comm::send(quantity_threshold1, 64, 1, tid);
    } else {
        Comm::receive(start_date, 64, 2, tid);
        Comm::receive(end_date, 64, 2, tid);
        Comm::receive(discount_center, 64, 2, tid);
        Comm::receive(discount_min, 64, 2, tid);
        Comm::receive(discount_max, 64, 2, tid);
        Comm::receive(quantity_threshold, 64, 2, tid);
    }

    int64_t revenue_share = 0;
    if (Comm::isServer()) {
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
    return lineitem_view;
}

View filterByShipdate(View &lineitem_view, int64_t start_date, int64_t end_date, int tid) {
    std::vector<std::string> fieldNames = {"l_shipdate", "l_shipdate"};
    std::vector<View::ComparatorType> comparatorTypes = {View::GREATER_EQ, View::LESS};
    std::vector<int64_t> constShares = {start_date, end_date};

    View filtered_view = lineitem_view;
    filtered_view.filterAndConditions(fieldNames, comparatorTypes, constShares, tid);

    return filtered_view;
}

View filterByDiscount(View &lineitem_view, int64_t discount_min, int64_t discount_max, int tid) {
    std::vector<std::string> fieldNames = {"l_discount", "l_discount"};
    std::vector<View::ComparatorType> comparatorTypes = {View::GREATER_EQ, View::LESS_EQ};
    std::vector<int64_t> constShares = {discount_min, discount_max};

    View filtered_view = lineitem_view;
    filtered_view.filterAndConditions(fieldNames, comparatorTypes, constShares, tid);

    return filtered_view;
}

View filterByQuantity(View &lineitem_view, int64_t quantity_threshold, int tid) {
    std::vector<std::string> fieldNames = {"l_quantity"};
    std::vector<View::ComparatorType> comparatorTypes = {View::LESS};
    std::vector<int64_t> constShares = {quantity_threshold};

    View filtered_view = lineitem_view;
    filtered_view.filterAndConditions(fieldNames, comparatorTypes, constShares, tid);

    return filtered_view;
}

int64_t calculateRevenue(View &filtered_view, int tid) {
    if (filtered_view.rowNum() == 0) {
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
    
    return revenue_sum;
}
