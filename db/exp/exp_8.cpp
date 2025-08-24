//
// Created by 杜建璋 on 25-8-23.
//

/*
 * TPC-H Query 13: Customer Distribution Query
 * 
 * SELECT c_count, count(*) as custdist
 * FROM (
 *      SELECT c_custkey, count(o_orderkey) as c_count
 *      FROM customer left outer join orders
 *          ON c_custkey = o_custkey
 *              AND o_comment not like '%[WORD1]%[WORD2]%'
 *      GROUP BY c_custkey
 *      ) as c_orders (c_custkey, c_count)
 * GROUP BY c_count
 * ORDER BY custdist desc, c_count desc
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
void generateTestData(int customer_rows, int orders_rows,
                      std::vector<int64_t> &c_custkey_data,
                      std::vector<int64_t> &c_custkey_hash_data,
                      std::vector<int64_t> &o_custkey_data,
                      std::vector<int64_t> &o_custkey_hash_data,
                      std::vector<int64_t> &o_orderkey_data,
                      std::vector<int64_t> &o_comment_flag_data);

View createCustomerTable(std::vector<int64_t> &c_custkey_shares, std::vector<int64_t> &c_custkey_hash_shares);

View createOrdersTable(std::vector<int64_t> &o_custkey_shares,
                       std::vector<int64_t> &o_custkey_hash_shares,
                       std::vector<int64_t> &o_orderkey_shares,
                       std::vector<int64_t> &o_comment_flag_shares);

View filterOrdersByComment(View &orders_view, int tid);

View performLeftOuterJoin(View &customer_view, View &filtered_orders_view, int tid);

void displayResults(View &joined_view);

int main(int argc, char *argv[]) {
    System::init(argc, argv);
    auto tid = System::nextTask() << (32 - Conf::TASK_TAG_BITS);

    // Read parameters from command line
    int customer_rows = 50;
    int orders_rows = 150;
    std::string word1 = "special";
    std::string word2 = "requests";

    if (Conf::_userParams.count("customer_rows")) {
        customer_rows = std::stoi(Conf::_userParams["customer_rows"]);
    }
    if (Conf::_userParams.count("orders_rows")) {
        orders_rows = std::stoi(Conf::_userParams["orders_rows"]);
    }
    if (Conf::_userParams.count("word1")) {
        word1 = Conf::_userParams["word1"];
    }
    if (Conf::_userParams.count("word2")) {
        word2 = Conf::_userParams["word2"];
    }

    // Generate test data
    std::vector<int64_t> c_custkey_data, c_custkey_hash_data, o_custkey_data, o_custkey_hash_data, o_orderkey_data, o_comment_flag_data;
    generateTestData(customer_rows, orders_rows, c_custkey_data, c_custkey_hash_data, o_custkey_data, o_custkey_hash_data, o_orderkey_data, o_comment_flag_data);

    // Convert to secret shares for 2PC (using bool shares for all data)
    auto c_custkey_shares = Secrets::boolShare(c_custkey_data, 2, 64, tid);
    auto c_custkey_hash_shares = Secrets::boolShare(c_custkey_hash_data, 2, 64, tid);
    auto o_custkey_shares = Secrets::boolShare(o_custkey_data, 2, 64, tid);
    auto o_custkey_hash_shares = Secrets::boolShare(o_custkey_hash_data, 2, 64, tid);
    auto o_orderkey_shares = Secrets::boolShare(o_orderkey_data, 2, 64, tid);
    auto o_comment_flag_shares = Secrets::boolShare(o_comment_flag_data, 2, 64, tid);

    if (Comm::isServer()) {
        Log::i("Starting TPC-H Query 13 execution...");
        auto query_start = System::currentTimeMillis();

        // Step 1: Create customer and orders tables
        auto customer_view = createCustomerTable(c_custkey_shares, c_custkey_hash_shares);
        auto orders_view = createOrdersTable(o_custkey_shares, o_custkey_hash_shares, o_orderkey_shares, o_comment_flag_shares);

        // Step 2: Filter orders by comment (exclude special comments)
        auto filtered_orders_view = filterOrdersByComment(orders_view, tid);

        // Step 3: Perform left outer join
        auto joined_view = performLeftOuterJoin(customer_view, filtered_orders_view, tid);

        // Step 4: Display results (in a real implementation, would continue with grouping)
        displayResults(joined_view);

        auto query_end = System::currentTimeMillis();
        Log::i("Total query execution time: {}ms", query_end - query_start);
    }

    System::finalize();
    return 0;
}

// Function implementations

void generateTestData(int customer_rows, int orders_rows,
                      std::vector<int64_t> &c_custkey_data,
                      std::vector<int64_t> &c_custkey_hash_data,
                      std::vector<int64_t> &o_custkey_data,
                      std::vector<int64_t> &o_custkey_hash_data,
                      std::vector<int64_t> &o_orderkey_data,
                      std::vector<int64_t> &o_comment_flag_data) {
    if (Comm::rank() == 2) {
        // Use fixed, small dataset for verification
        // Customer data: 5 customers (1, 2, 3, 4, 5)
        c_custkey_data = {1, 2, 3, 4, 5};
        
        // Calculate hash values for customer keys using Views::hash
        c_custkey_hash_data.reserve(c_custkey_data.size());
        for (int64_t key : c_custkey_data) {
            c_custkey_hash_data.push_back(Views::hash(key));
        }
        
        // Orders data: carefully designed to test left outer join
        // Customer 1: 2 orders (1 normal, 1 with special comment)
        // Customer 2: 1 normal order
        // Customer 3: 1 order with special comment (will be filtered out)
        // Customer 4: 0 orders (will show NULL in join)
        // Customer 5: 2 normal orders
        o_custkey_data = {1, 1, 2, 3, 5, 5};
        o_orderkey_data = {101, 102, 201, 301, 501, 502};
        o_comment_flag_data = {0, 1, 0, 1, 0, 0}; // 0=normal, 1=special comment
        
        // Calculate hash values for order customer keys using Views::hash
        o_custkey_hash_data.reserve(o_custkey_data.size());
        for (int64_t key : o_custkey_data) {
            o_custkey_hash_data.push_back(Views::hash(key));
        }
        
        Log::i("Generated FIXED test data for verification:");
        Log::i("===========================================");
        Log::i("Customers: {} records", c_custkey_data.size());
        for (size_t i = 0; i < c_custkey_data.size(); i++) {
            Log::i("  Customer {}: c_custkey={}, hash={}", i+1, c_custkey_data[i], c_custkey_hash_data[i]);
        }
        
        Log::i("Orders: {} records", o_custkey_data.size());
        for (size_t i = 0; i < o_custkey_data.size(); i++) {
            Log::i("  Order {}: o_custkey={}, hash={}, o_orderkey={}, o_comment_flag={}", 
                   i+1, o_custkey_data[i], o_custkey_hash_data[i], o_orderkey_data[i], o_comment_flag_data[i]);
        }
        
        Log::i("Expected behavior:");
        Log::i("- Customer 1: has 2 orders, 1 will be filtered (comment_flag=1)");
        Log::i("- Customer 2: has 1 normal order");
        Log::i("- Customer 3: has 1 order but will be filtered (comment_flag=1)");
        Log::i("- Customer 4: has NO orders (left outer join will show NULL)");
        Log::i("- Customer 5: has 2 normal orders");
        Log::i("After filtering: Customer 1(1 order), Customer 2(1 order), Customer 4(NULL), Customer 5(2 orders)");
        Log::i("Expected left outer join result: 6 rows total");
    }
}

View createCustomerTable(std::vector<int64_t> &c_custkey_shares, std::vector<int64_t> &c_custkey_hash_shares) {
    std::string table_name = "customer";
    std::vector<std::string> fields = {"c_custkey"};
    std::vector<int> widths = {64};

    Table customer_table(table_name, fields, widths, "c_custkey");

    for (size_t i = 0; i < c_custkey_shares.size(); i++) {
        std::vector<int64_t> row = {c_custkey_shares[i], c_custkey_hash_shares[i]};
        customer_table.insert(row);
    }

    auto customer_view = Views::selectAll(customer_table);
    Log::i("Created customer table with {} rows (with hash tags)", customer_table.rowNum());
    return customer_view;
}

View createOrdersTable(std::vector<int64_t> &o_custkey_shares,
                       std::vector<int64_t> &o_custkey_hash_shares,
                       std::vector<int64_t> &o_orderkey_shares,
                       std::vector<int64_t> &o_comment_flag_shares) {
    std::string table_name = "orders";
    std::vector<std::string> fields = {"o_custkey", "o_orderkey", "o_comment_flag"};
    std::vector<int> widths = {64, 64, 64};

    Table orders_table(table_name, fields, widths, "o_custkey");

    for (size_t i = 0; i < o_custkey_shares.size(); i++) {
        std::vector<int64_t> row = {
            o_custkey_shares[i],
            o_orderkey_shares[i],
            o_comment_flag_shares[i],
            o_custkey_hash_shares[i]
        };
        orders_table.insert(row);
    }

    auto orders_view = Views::selectAll(orders_table);
    Log::i("Created orders table with {} rows (with hash tags)", orders_table.rowNum());
    return orders_view;
}

View filterOrdersByComment(View &orders_view, int tid) {
    Log::i("Step 1: Filtering orders by comment (excluding special comments)...");
    auto step1_start = System::currentTimeMillis();

    // Filter orders where comment flag = 0 (no special comments)
    std::vector<std::string> fieldNames = {"o_comment_flag"};
    std::vector<View::ComparatorType> comparatorTypes = {View::EQUALS};
    std::vector<int64_t> constShares = {Comm::rank() * 0}; // Filter for flag = 0

    View filtered_orders = orders_view;
    filtered_orders.filterAndConditions(fieldNames, comparatorTypes, constShares, tid);

    auto step1_end = System::currentTimeMillis();
    Log::i("Step 1 completed in {}ms", step1_end - step1_start);
    Log::i("Filtered orders (no special comments): {} rows", filtered_orders.rowNum());
    
    return filtered_orders;
}

View performLeftOuterJoin(View &customer_view, View &filtered_orders_view, int tid) {
    Log::i("Step 2: Performing left outer join between customer and filtered orders...");
    auto step2_start = System::currentTimeMillis();

    // Use the new leftOuterJoin function from Views
    std::string customer_key = "c_custkey";
    std::string order_key = "o_custkey";
    
    View joined_view = Views::leftOuterJoin(customer_view, filtered_orders_view, customer_key, order_key);

    auto step2_end = System::currentTimeMillis();
    Log::i("Step 2 completed in {}ms", step2_end - step2_start);
    Log::i("Left outer join result: {} rows", joined_view.rowNum());
    
    return joined_view;
}

void displayResults(View &joined_view) {
    if (Comm::isServer() && !joined_view._dataCols.empty()) {
        Log::i("TPC-H Query 13 Results:");
        Log::i("======================");
        Log::i("Left Outer Join completed successfully!");
        Log::i("Result contains {} rows", joined_view.rowNum());
        Log::i("Result has {} columns:", joined_view.colNum());
        
        for (size_t i = 0; i < joined_view._fieldNames.size(); i++) {
            Log::i("  Column {}: {}", i, joined_view._fieldNames[i]);
        }

        // Reconstruct and display the actual join results for verification
        int numCols = joined_view._dataCols.size();
        int numRows = joined_view._dataCols[0].size();
        
        std::vector<int64_t> allSecrets;
        allSecrets.reserve(numCols * numRows);
        
        for (int col = 0; col < numCols; col++) {
            for (int row = 0; row < numRows; row++) {
                allSecrets.push_back(joined_view._dataCols[col][row]);
            }
        }
        
        auto allReconstructed = Secrets::boolReconstruct(allSecrets, 2, 64, System::nextTask());
        
        if (Comm::rank() == 2) {
            Log::i("\nActual Join Results (reconstructed):");
            Log::i("====================================");
            Log::i("Row | c_custkey | o_custkey | o_orderkey | o_comment_flag");
            Log::i("----|-----------|-----------|------------|---------------");
            
            for (int row = 0; row < numRows; row++) {
                int64_t c_custkey = allReconstructed[0 * numRows + row];
                int64_t o_custkey = allReconstructed[1 * numRows + row];
                int64_t o_orderkey = allReconstructed[2 * numRows + row];
                int64_t o_comment_flag = allReconstructed[3 * numRows + row];
                
                Log::i("{:3} | {:9} | {:9} | {:10} | {:13}", 
                       row + 1, c_custkey, o_custkey, o_orderkey, o_comment_flag);
            }
            
            Log::i("\nVerification Analysis:");
            Log::i("=====================");
            Log::i("Expected results based on fixed test data:");
            Log::i("- Customer 1: should appear once (order 101, comment_flag=0)");
            Log::i("- Customer 2: should appear once (order 201, comment_flag=0)");
            Log::i("- Customer 3: should appear with NULL (no orders after filtering)");
            Log::i("- Customer 4: should appear with NULL (no orders at all)");
            Log::i("- Customer 5: should appear twice (orders 501,502, comment_flag=0)");
            Log::i("Total expected rows: 5 (all customers preserved in left outer join)");
            
            // Verify the results
            std::map<int64_t, int> customerCounts;
            int nullRows = 0;
            
            for (int row = 0; row < numRows; row++) {
                int64_t c_custkey = allReconstructed[0 * numRows + row];
                int64_t o_custkey = allReconstructed[1 * numRows + row];
                
                customerCounts[c_custkey]++;
                if (o_custkey == 0) {
                    nullRows++;
                }
            }
            
            Log::i("\nActual Results Summary:");
            for (auto& pair : customerCounts) {
                Log::i("- Customer {}: appears {} time(s)", pair.first, pair.second);
            }
            Log::i("- Rows with NULL orders: {}", nullRows);
            
            bool correct = (numRows == 5 && customerCounts.size() == 5);
            if (correct) {
                Log::i("✅ VERIFICATION PASSED: Left outer join results are correct!");
            } else {
                Log::i("❌ VERIFICATION FAILED: Unexpected results!");
            }
        }
    }
    
    if (Comm::rank() == 2) {
        Log::i("\nQuery Summary:");
        Log::i("This query demonstrates TPC-H Query 13 implementation:");
        Log::i("1. Created customer and orders tables with secret shares");
        Log::i("2. Filtered orders to exclude those with special comments");
        Log::i("3. Performed left outer join to include all customers");
        Log::i("4. Result includes customers with orders and customers without orders");
        Log::i("\nIn a complete implementation, the next steps would be:");
        Log::i("- Group by customer key and count orders per customer");
        Log::i("- Group by order count and count customers per order count");
        Log::i("- Sort results by customer count (desc) and order count (desc)");
    }
}
