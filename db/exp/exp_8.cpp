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
 *              AND o_comment not like '%[word]%[WORD2]%'
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
#include "utils/Log.h"
#include "utils/Math.h"
#include "parallel/ThreadPoolSupport.h"

#include <string>
#include <vector>
#include <map>

#include "conf/DbConf.h"

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

View filterOrdersByComment(View &orders_view, int64_t word, int tid);

View performLeftOuterJoin(View &customer_view, View &filtered_orders_view, int tid);

int main(int argc, char *argv[]) {
    System::init(argc, argv);
    DbConf::init();
    auto tid = System::nextTask() << (32 - Conf::TASK_TAG_BITS);

    // Read parameters from command line
    int customer_rows = 50;
    int orders_rows = 150;

    if (Conf::_userParams.count("rows1")) {
        customer_rows = std::stoi(Conf::_userParams["customer_rows"]);
    }
    if (Conf::_userParams.count("rows2")) {
        orders_rows = std::stoi(Conf::_userParams["orders_rows"]);
    }

    if (Comm::isClient()) {
        Log::i("Data size: customer: {} orders: {}", customer_rows, orders_rows);
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

    // Generate and distribute constant parameters as secret shares
    int64_t word;
    if (Comm::isClient()) {
        int64_t word0 = Math::randInt();
        int64_t word1 = Math::randInt();
        
        Comm::send(word0, 64, 0, tid);
        Comm::send(word1, 64, 1, tid);
    } else {
        Comm::receive(word, 64, 2, tid);
    }

    if (Comm::isServer()) {
        auto query_start = System::currentTimeMillis();

        // Step 1: Create customer and orders tables
        auto customer_view = createCustomerTable(c_custkey_shares, c_custkey_hash_shares);
        auto orders_view = createOrdersTable(o_custkey_shares, o_custkey_hash_shares, o_orderkey_shares, o_comment_flag_shares);

        // Step 2: Filter orders by comment (exclude special comments)
        auto filtered_orders_view = filterOrdersByComment(orders_view, word, tid);

        // Step 3: Perform left outer join
        auto joined_view = performLeftOuterJoin(customer_view, filtered_orders_view, tid);

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
        // Generate random customer data
        c_custkey_data.reserve(customer_rows);
        c_custkey_hash_data.reserve(customer_rows);
        
        for (int i = 0; i < customer_rows; i++) {
            int64_t custkey = Math::randInt();
            c_custkey_data.push_back(custkey);
            c_custkey_hash_data.push_back(Views::hash(custkey));
        }
        
        // Generate random orders data
        o_custkey_data.reserve(orders_rows);
        o_custkey_hash_data.reserve(orders_rows);
        o_orderkey_data.reserve(orders_rows);
        o_comment_flag_data.reserve(orders_rows);
        
        for (int i = 0; i < orders_rows; i++) {
            int64_t custkey = Math::randInt();
            int64_t orderkey = Math::randInt();
            int64_t comment_flag = Math::randInt() % 2; // 0 or 1
            
            o_custkey_data.push_back(custkey);
            o_custkey_hash_data.push_back(Views::hash(custkey));
            o_orderkey_data.push_back(orderkey);
            o_comment_flag_data.push_back(comment_flag);
        }
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
    return orders_view;
}

View filterOrdersByComment(View &orders_view, int64_t word, int tid) {
    // Filter orders where comment flag = 0 (no special comments)
    std::vector<std::string> fieldNames = {"o_comment_flag"};
    std::vector<View::ComparatorType> comparatorTypes = {View::EQUALS};
    std::vector<int64_t> constShares = {word}; // Filter for flag = 0

    View filtered_orders = orders_view;
    filtered_orders.filterAndConditions(fieldNames, comparatorTypes, constShares, tid);
    
    return filtered_orders;
}

View performLeftOuterJoin(View &customer_view, View &filtered_orders_view, int tid) {
    // Use the new leftOuterJoin function from Views
    std::string customer_key = "c_custkey";
    std::string order_key = "o_custkey";
    
    View joined_view = Views::leftOuterJoin(customer_view, filtered_orders_view, customer_key, order_key);
    
    return joined_view;
}
