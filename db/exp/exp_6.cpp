//
// Created by 杜建璋 on 25-8-21.
//

/*
* SELECT o_orderpriority, count(*) as order_count
* FROM orders
* WHERE o_orderdate >= date '[DATE]'
*   AND o_orderdate < date '[DATE]' + interval '3' month
*   AND EXISTS (
*           SELECT *
*           FROM lineitem
*           WHERE l_orderkey = o_orderkey
*           AND l_commitdate < l_receiptdate
*           )
* GROUP BY o_orderpriority
* ORDER BY o_orderpriority
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
void generateTestData(int orders_rows, int lineitem_rows,
                      std::vector<int64_t> &o_orderkey_data,
                      std::vector<int64_t> &o_orderpriority_data,
                      std::vector<int64_t> &o_orderdate_data,
                      std::vector<int64_t> &l_orderkey_data,
                      std::vector<int64_t> &l_commitdate_data,
                      std::vector<int64_t> &l_receiptdate_data);

View createOrdersTable(std::vector<int64_t> &o_orderkey_shares,
                       std::vector<int64_t> &o_orderpriority_shares,
                       std::vector<int64_t> &o_orderdate_shares);

View createLineitemTable(std::vector<int64_t> &l_orderkey_shares,
                         std::vector<int64_t> &l_commitdate_shares,
                         std::vector<int64_t> &l_receiptdate_shares);

View filterOrdersByDate(View &orders_view, int64_t start_date, int64_t end_date, int tid);

View filterLineitemByCommitDate(View &lineitem_view, int tid);

std::vector<int64_t> executeExistsClause(View &filtered_orders, View &filtered_lineitem, int tid);

View filterOrdersByExists(View &filtered_orders, std::vector<int64_t> &exists_results, int tid);

View executeGroupByCount(View &final_orders, int tid);

View executeSortByPriority(View &result_view, int tid);

void displayResults(View &result_view, int tid);

int main(int argc, char *argv[]) {
    System::init(argc, argv);
    auto tid = System::nextTask() << (32 - Conf::TASK_TAG_BITS);

    // Read parameters from command line
    int orders_rows = 1000;
    int lineitem_rows = 3000;
    int64_t start_date = 19940101; // 1994-01-01
    int64_t end_date = 19940401;   // 1994-04-01 (3 months later)

    if (Conf::_userParams.count("orders_rows")) {
        orders_rows = std::stoi(Conf::_userParams["orders_rows"]);
    }
    if (Conf::_userParams.count("lineitem_rows")) {
        lineitem_rows = std::stoi(Conf::_userParams["lineitem_rows"]);
    }
    if (Conf::_userParams.count("start_date")) {
        start_date = std::stoll(Conf::_userParams["start_date"]);
    }
    if (Conf::_userParams.count("end_date")) {
        end_date = std::stoll(Conf::_userParams["end_date"]);
    }

    // Generate test data
    std::vector<int64_t> o_orderkey_data, o_orderpriority_data, o_orderdate_data;
    std::vector<int64_t> l_orderkey_data, l_commitdate_data, l_receiptdate_data;
    generateTestData(orders_rows, lineitem_rows,
                     o_orderkey_data, o_orderpriority_data, o_orderdate_data,
                     l_orderkey_data, l_commitdate_data, l_receiptdate_data);

    // Convert to secret shares for 2PC
    auto o_orderkey_shares = Secrets::boolShare(o_orderkey_data, 2, 64, tid);
    auto o_orderpriority_shares = Secrets::boolShare(o_orderpriority_data, 2, 64, tid);
    auto o_orderdate_shares = Secrets::boolShare(o_orderdate_data, 2, 64, tid);
    auto l_orderkey_shares = Secrets::boolShare(l_orderkey_data, 2, 64, tid);
    auto l_commitdate_shares = Secrets::boolShare(l_commitdate_data, 2, 64, tid);
    auto l_receiptdate_shares = Secrets::boolShare(l_receiptdate_data, 2, 64, tid);

    View result_view;
    if (Comm::isServer()) {
        Log::i("Starting TPC-H Query 4 execution...");
        auto query_start = System::currentTimeMillis();

        // Create tables
        auto orders_view = createOrdersTable(o_orderkey_shares, o_orderpriority_shares, o_orderdate_shares);
        auto lineitem_view = createLineitemTable(l_orderkey_shares, l_commitdate_shares, l_receiptdate_shares);

        // Step 1: Filter orders by date range
        auto filtered_orders = filterOrdersByDate(orders_view, start_date, end_date, tid);

        // Step 2: Filter lineitem by commit date < receipt date
        auto filtered_lineitem = filterLineitemByCommitDate(lineitem_view, tid);

        // Step 3: Execute EXISTS clause (semi-join)
        auto exists_results = executeExistsClause(filtered_orders, filtered_lineitem, tid);

        // Step 4: Filter orders by EXISTS results
        auto final_orders = filterOrdersByExists(filtered_orders, exists_results, tid);

        // Step 5: GROUP BY o_orderpriority and COUNT(*)
        result_view = executeGroupByCount(final_orders, tid);

        // Step 6: ORDER BY o_orderpriority
        result_view = executeSortByPriority(result_view, tid);

        auto query_end = System::currentTimeMillis();
        Log::i("Total query execution time: {}ms", query_end - query_start);
    }

    // Display results
    displayResults(result_view, tid);

    System::finalize();
    return 0;
}

// Function implementations

void generateTestData(int orders_rows, int lineitem_rows,
                      std::vector<int64_t> &o_orderkey_data,
                      std::vector<int64_t> &o_orderpriority_data,
                      std::vector<int64_t> &o_orderdate_data,
                      std::vector<int64_t> &l_orderkey_data,
                      std::vector<int64_t> &l_commitdate_data,
                      std::vector<int64_t> &l_receiptdate_data) {
    if (Comm::rank() == 2) {
        o_orderkey_data.reserve(orders_rows);
        o_orderpriority_data.reserve(orders_rows);
        o_orderdate_data.reserve(orders_rows);
        l_orderkey_data.reserve(lineitem_rows);
        l_commitdate_data.reserve(lineitem_rows);
        l_receiptdate_data.reserve(lineitem_rows);

        // Generate orders data
        // Priority levels: 1-URGENT, 2-HIGH, 3-MEDIUM, 4-NOT SPECIFIED, 5-LOW
        std::vector<int64_t> priorities = {1, 2, 3, 4, 5};
        
        for (int i = 0; i < orders_rows; i++) {
            int64_t orderkey = i + 1;
            int64_t priority = priorities[i % 5];
            int64_t orderdate = 19940101 + (i % 120); // Dates in range 1994-01-01 to 1994-04-30
            
            o_orderkey_data.push_back(orderkey);
            o_orderpriority_data.push_back(priority);
            o_orderdate_data.push_back(orderdate);
        }

        // Generate lineitem data
        for (int i = 0; i < lineitem_rows; i++) {
            int64_t orderkey = (i % orders_rows) + 1; // Reference to orders
            int64_t commitdate = 19940101 + (i % 100);
            int64_t receiptdate = commitdate + (i % 2 == 0 ? 5 : -2); // Some commit < receipt, some not
            
            l_orderkey_data.push_back(orderkey);
            l_commitdate_data.push_back(commitdate);
            l_receiptdate_data.push_back(receiptdate);
        }

        Log::i("Generated {} orders and {} lineitem records", orders_rows, lineitem_rows);
        Log::i("Order priorities: 1-URGENT, 2-HIGH, 3-MEDIUM, 4-NOT SPECIFIED, 5-LOW");
        Log::i("Date range: 1994-01-01 to 1994-04-30");
        Log::i("Some lineitems have commitdate < receiptdate, others don't");
    }
}

View createOrdersTable(std::vector<int64_t> &o_orderkey_shares,
                       std::vector<int64_t> &o_orderpriority_shares,
                       std::vector<int64_t> &o_orderdate_shares) {
    std::string table_name = "orders";
    std::vector<std::string> fields = {"o_orderkey", "o_orderpriority", "o_orderdate"};
    std::vector<int> widths = {64, 64, 64};

    Table orders_table(table_name, fields, widths, "");

    for (size_t i = 0; i < o_orderkey_shares.size(); i++) {
        std::vector<int64_t> row = {
            o_orderkey_shares[i],
            o_orderpriority_shares[i],
            o_orderdate_shares[i]
        };
        orders_table.insert(row);
    }

    auto orders_view = Views::selectAll(orders_table);
    Log::i("Created orders table with {} rows", orders_table.rowNum());
    return orders_view;
}

View createLineitemTable(std::vector<int64_t> &l_orderkey_shares,
                         std::vector<int64_t> &l_commitdate_shares,
                         std::vector<int64_t> &l_receiptdate_shares) {
    std::string table_name = "lineitem";
    std::vector<std::string> fields = {"l_orderkey", "l_commitdate", "l_receiptdate"};
    std::vector<int> widths = {64, 64, 64};

    Table lineitem_table(table_name, fields, widths, "");

    for (size_t i = 0; i < l_orderkey_shares.size(); i++) {
        std::vector<int64_t> row = {
            l_orderkey_shares[i],
            l_commitdate_shares[i],
            l_receiptdate_shares[i]
        };
        lineitem_table.insert(row);
    }

    auto lineitem_view = Views::selectAll(lineitem_table);
    Log::i("Created lineitem table with {} rows", lineitem_table.rowNum());
    return lineitem_view;
}

View filterOrdersByDate(View &orders_view, int64_t start_date, int64_t end_date, int tid) {
    Log::i("Step 1: Filtering orders by date range [{}, {})...", start_date, end_date);
    auto step1_start = System::currentTimeMillis();

    std::vector<std::string> fieldNames = {"o_orderdate", "o_orderdate"};
    std::vector<View::ComparatorType> comparatorTypes = {View::GREATER_EQ, View::LESS};
    std::vector<int64_t> constShares = {Comm::rank() * start_date, Comm::rank() * end_date};

    View filtered_view = orders_view;
    filtered_view.filterAndConditions(fieldNames, comparatorTypes, constShares, tid);

    auto step1_end = System::currentTimeMillis();
    Log::i("Step 1 completed in {}ms", step1_end - step1_start);
    Log::i("Filtered orders table has {} rows", filtered_view.rowNum());
    return filtered_view;
}

View filterLineitemByCommitDate(View &lineitem_view, int tid) {
    Log::i("Step 2: Filtering lineitem by l_commitdate < l_receiptdate...");
    auto step2_start = System::currentTimeMillis();

    if (lineitem_view.rowNum() == 0) {
        auto step2_end = System::currentTimeMillis();
        Log::i("Step 2 completed in {}ms (empty input)", step2_end - step2_start);
        return lineitem_view;
    }

    // Get the column data for comparison
    auto &commitdate_col = lineitem_view._dataCols[lineitem_view.colIndex("l_commitdate")];
    auto &receiptdate_col = lineitem_view._dataCols[lineitem_view.colIndex("l_receiptdate")];

    // Perform field-to-field comparison: l_commitdate < l_receiptdate
    // Since data is stored as bool shares, use BoolLessBatchOperator directly
    std::vector<int64_t> comparison_result;
    
    if (Conf::BATCH_SIZE <= 0 || Conf::DISABLE_MULTI_THREAD) {
        // Single batch processing
        comparison_result = BoolLessBatchOperator(&commitdate_col, &receiptdate_col, 64, tid, 0,
                                                  SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    } else {
        // Multi-batch processing
        size_t data_size = commitdate_col.size();
        int batchSize = Conf::BATCH_SIZE;
        int batchNum = (data_size + batchSize - 1) / batchSize;

        std::vector<std::future<std::vector<int64_t>>> batch_futures(batchNum);

        for (int b = 0; b < batchNum; ++b) {
            batch_futures[b] = ThreadPoolSupport::submit([&, b]() -> std::vector<int64_t> {
                int start = b * batchSize;
                int end = std::min(start + batchSize, static_cast<int>(data_size));

                std::vector<int64_t> batch_commitdate(commitdate_col.begin() + start, commitdate_col.begin() + end);
                std::vector<int64_t> batch_receiptdate(receiptdate_col.begin() + start, receiptdate_col.begin() + end);

                int batch_tid = tid + b * BoolLessBatchOperator::tagStride();

                return BoolLessBatchOperator(&batch_commitdate, &batch_receiptdate, 64, batch_tid, 0,
                                             SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
            });
        }

        comparison_result.reserve(data_size);
        for (auto &f : batch_futures) {
            auto batch_res = f.get();
            comparison_result.insert(comparison_result.end(), batch_res.begin(), batch_res.end());
        }
    }

    // Apply the filter condition
    View filtered_view = lineitem_view;
    filtered_view._dataCols[filtered_view.colNum() + View::VALID_COL_OFFSET] = comparison_result;
    filtered_view.clearInvalidEntries(tid + BoolLessBatchOperator::tagStride());

    auto step2_end = System::currentTimeMillis();
    Log::i("Step 2 completed in {}ms", step2_end - step2_start);
    Log::i("Filtered lineitem table has {} rows", filtered_view.rowNum());
    return filtered_view;
}

std::vector<int64_t> executeExistsClause(View &filtered_orders, View &filtered_lineitem, int tid) {
    Log::i("Step 3: Executing EXISTS clause (semi-join on orderkey)...");
    auto step3_start = System::currentTimeMillis();

    auto orders_orderkey_col = filtered_orders._dataCols[0]; // o_orderkey
    auto lineitem_orderkey_col = filtered_lineitem._dataCols[0]; // l_orderkey

    auto exists_results = Views::in(orders_orderkey_col, lineitem_orderkey_col);

    auto step3_end = System::currentTimeMillis();
    Log::i("Step 3 completed in {}ms", step3_end - step3_start);

    return exists_results;
}

View filterOrdersByExists(View &filtered_orders, std::vector<int64_t> &exists_results, int tid) {
    Log::i("Step 4: Filtering orders by EXISTS results...");
    auto step4_start = System::currentTimeMillis();

    View final_orders = filtered_orders;
    final_orders._dataCols[final_orders.colNum() + View::VALID_COL_OFFSET] = exists_results;
    final_orders.clearInvalidEntries(tid);

    auto step4_end = System::currentTimeMillis();
    Log::i("Step 4 completed in {}ms", step4_end - step4_start);
    Log::i("Final orders table has {} rows", final_orders.rowNum());
    return final_orders;
}

View executeGroupByCount(View &final_orders, int tid) {
    Log::i("Step 5: Executing GROUP BY o_orderpriority and COUNT(*)...");
    auto step5_start = System::currentTimeMillis();

    if (final_orders.rowNum() == 0) {
        std::vector<std::string> result_fields = {"o_orderpriority", "order_count"};
        std::vector<int> result_widths = {64, 64};
        View result_view(result_fields, result_widths);

        auto step5_end = System::currentTimeMillis();
        Log::i("Step 5 completed in {}ms (empty input)", step5_end - step5_start);
        return result_view;
    }

    std::string priority_field = "o_orderpriority";
    auto group_heads = final_orders.groupBy(priority_field, tid);

    std::vector<std::string> group_fields = {priority_field};
    final_orders.count(group_fields, group_heads, "order_count", tid);

    auto step5_end = System::currentTimeMillis();
    Log::i("Step 5 completed in {}ms", step5_end - step5_start);
    Log::i("Grouped result has {} rows", final_orders.rowNum());
    return final_orders;
}

View executeSortByPriority(View &result_view, int tid) {
    Log::i("Step 6: Executing ORDER BY o_orderpriority...");
    auto step6_start = System::currentTimeMillis();

    if (result_view.rowNum() == 0) {
        auto step6_end = System::currentTimeMillis();
        Log::i("Step 6 completed in {}ms (empty input)", step6_end - step6_start);
        return result_view;
    }

    std::string priority_field = "o_orderpriority";
    result_view.sort(priority_field, true, tid); // true for ascending order

    auto step6_end = System::currentTimeMillis();
    Log::i("Step 6 completed in {}ms", step6_end - step6_start);
    Log::i("Final sorted result has {} rows", result_view.rowNum());
    return result_view;
}

void displayResults(View &result_view, int tid) {
    Log::i("Reconstructing results for verification...");
    std::vector<int64_t> priority_col, count_col;
    if (Comm::isServer() && !result_view._dataCols.empty()) {
        priority_col = result_view._dataCols[0]; // o_orderpriority
        count_col = result_view._dataCols[1];    // order_count
    }

    auto priority_plain = Secrets::boolReconstruct(priority_col, 2, 64, tid);
    auto count_plain = Secrets::boolReconstruct(count_col, 2, 64, tid);

    if (Comm::rank() == 2) {
        Log::i("TPC-H Query 4 Results:");
        Log::i("o_orderpriority\torder_count");
        Log::i("--------------\t-----------");

        for (size_t i = 0; i < priority_plain.size(); i++) {
            std::string priority_name;
            switch (priority_plain[i]) {
                case 1: priority_name = "1-URGENT"; break;
                case 2: priority_name = "2-HIGH"; break;
                case 3: priority_name = "3-MEDIUM"; break;
                case 4: priority_name = "4-NOT SPECIFIED"; break;
                case 5: priority_name = "5-LOW"; break;
                default: priority_name = std::to_string(priority_plain[i]); break;
            }
            Log::i("{}\t\t{}", priority_name, count_plain[i]);
        }

        Log::i("\nQuery Summary:");
        Log::i("This query counts orders by priority level where:");
        Log::i("1. Order date is within the specified range");
        Log::i("2. At least one lineitem exists with commitdate < receiptdate");
        Log::i("Results are ordered by priority level (1-URGENT to 5-LOW)");
    }
}
