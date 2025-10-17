


#include "secret/Secrets.h"
#include "utils/System.h"

#include "../include/basis/View.h"
#include "../include/basis/Views.h"
#include "../include/basis/Table.h"
#include "../include/operator/SelectSupport.h"
#include "utils/Log.h"
#include "utils/Math.h"
#include "compute/batch/bool/BoolLessBatchOperator.h"
#include "parallel/ThreadPoolSupport.h"

#include <string>
#include <vector>
#include <random>
#include <algorithm>
#include <map>
#include <set>

#include "compute/batch/bool/BoolAndBatchOperator.h"
#include "conf/DbConf.h"
#include "utils/StringUtils.h"

static int64_t PLAIN_START_DATE = Math::randInt();
static int64_t PLAIN_END_DATE   = PLAIN_START_DATE + 3;

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

int main(int argc, char *argv[]) {
    System::init(argc, argv);
    DbConf::init();
    auto tid = System::nextTask() << (32 - Conf::TASK_TAG_BITS);

    int orders_rows = 1000;
    int lineitem_rows = 1000;

    if (Conf::_userParams.count("rows1")) {
        orders_rows = std::stoi(Conf::_userParams["rows1"]);
    }
    if (Conf::_userParams.count("rows2")) {
        lineitem_rows = std::stoi(Conf::_userParams["rows2"]);
    }

    if (Comm::isClient()) {
        Log::i("Data size: orders: {} lineitem: {}", orders_rows, lineitem_rows);
    }

    std::vector<int64_t> o_orderkey_data, o_orderpriority_data, o_orderdate_data;
    std::vector<int64_t> l_orderkey_data, l_commitdate_data, l_receiptdate_data;
    generateTestData(orders_rows, lineitem_rows, o_orderkey_data, o_orderpriority_data, o_orderdate_data,
                     l_orderkey_data, l_commitdate_data, l_receiptdate_data);

    auto o_orderkey_shares = Secrets::boolShare(o_orderkey_data, 2, 64, tid);
    auto o_orderpriority_shares = Secrets::boolShare(o_orderpriority_data, 2, 64, tid);
    auto o_orderdate_shares = Secrets::boolShare(o_orderdate_data, 2, 64, tid);
    auto l_orderkey_shares = Secrets::boolShare(l_orderkey_data, 2, 64, tid);
    auto l_commitdate_shares = Secrets::boolShare(l_commitdate_data, 2, 64, tid);
    auto l_receiptdate_shares = Secrets::boolShare(l_receiptdate_data, 2, 64, tid);

    int64_t start_date, end_date;
    if (Comm::isClient()) {
        int64_t rand_int0 = Math::randInt();
        int64_t start_date0 = PLAIN_START_DATE ^ rand_int0;
        int64_t start_date1 = rand_int0;
        int64_t rand_int1 = Math::randInt();
        int64_t end_date0 = PLAIN_END_DATE ^ rand_int1;
        int64_t end_date1 = rand_int1;
        Comm::send(start_date0, 64, 0, tid);
        Comm::send(end_date0, 64, 0, tid);
        Comm::send(start_date1, 64, 1, tid);
        Comm::send(end_date1, 64, 1, tid);
    } else {
        Comm::receive(start_date, 64, 2, tid);
        Comm::receive(end_date, 64, 2, tid);
    }

    View result_view;
    if (Comm::isServer()) {
        auto orders_view = createOrdersTable(o_orderkey_shares, o_orderpriority_shares, o_orderdate_shares);
        auto lineitem_view = createLineitemTable(l_orderkey_shares, l_commitdate_shares, l_receiptdate_shares);

        auto query_start = System::currentTimeMillis();

        View filtered_orders, filtered_lineitem;
        if (DbConf::BASELINE_MODE) {
            filtered_orders = filterOrdersByDate(orders_view, start_date, end_date, tid);
            filtered_lineitem = filterLineitemByCommitDate(lineitem_view, tid);
        } else {
            auto f = ThreadPoolSupport::submit([&] {
                return filterOrdersByDate(orders_view, start_date, end_date, tid);
            });
            filtered_lineitem = filterLineitemByCommitDate(lineitem_view, tid + 10000);
            filtered_orders = f.get();
        }

        auto exists_results = executeExistsClause(filtered_orders, filtered_lineitem, tid);
        auto final_orders = filterOrdersByExists(filtered_orders, exists_results, tid);

        result_view = executeGroupByCount(final_orders, tid);


        auto query_end = System::currentTimeMillis();
        Log::i("Total query execution time: {}ms", query_end - query_start);
    }

    System::finalize();
    return 0;
}


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


        for (int i = 0; i < orders_rows; i++) {
            int64_t orderkey = Math::randInt();
            int64_t priority = Math::randInt();
            int64_t orderdate = Math::randInt();

            o_orderkey_data.push_back(orderkey);
            o_orderpriority_data.push_back(priority);
            o_orderdate_data.push_back(orderdate);
        }

        for (int i = 0; i < lineitem_rows; i++) {
            int64_t orderkey = Math::randInt();
            int64_t commitdate = Math::randInt();
            int64_t receiptdate = Math::randInt();

            l_orderkey_data.push_back(orderkey);
            l_commitdate_data.push_back(commitdate);
            l_receiptdate_data.push_back(receiptdate);
        }
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
    return lineitem_view;
}

View filterOrdersByDate(View &orders_view, int64_t start_date, int64_t end_date, int tid) {
    std::vector<std::string> fieldNames = {"o_orderdate", "o_orderdate"};
    std::vector<View::ComparatorType> comparatorTypes = {View::GREATER_EQ, View::LESS};
    std::vector<int64_t> constShares = {start_date, end_date};


    View filtered_view = orders_view;
    filtered_view.filterAndConditions(fieldNames, comparatorTypes, constShares, tid);

    return filtered_view;
}

View filterLineitemByCommitDate(View &lineitem_view, int tid) {
    if (lineitem_view.rowNum() == 0) {
        return lineitem_view;
    }

    auto &commitdate_col = lineitem_view._dataCols[lineitem_view.colIndex("l_commitdate")];
    auto &receiptdate_col = lineitem_view._dataCols[lineitem_view.colIndex("l_receiptdate")];

    std::vector<int64_t> comparison_result;

    if (Conf::BATCH_SIZE <= 0 || Conf::DISABLE_MULTI_THREAD) {
        comparison_result = BoolLessBatchOperator(&commitdate_col, &receiptdate_col, 64, tid, 0,
                                                  SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    } else {
        size_t data_size = commitdate_col.size();
        int batchSize = Conf::BATCH_SIZE;
        int batchNum = (data_size + batchSize - 1) / batchSize;

        std::vector<std::future<std::vector<int64_t> > > batch_futures(batchNum);

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
        for (auto &f: batch_futures) {
            auto batch_res = f.get();
            comparison_result.insert(comparison_result.end(), batch_res.begin(), batch_res.end());
        }
    }

    View filtered_view = lineitem_view;
    filtered_view._dataCols[filtered_view.colNum() + View::VALID_COL_OFFSET] = comparison_result;
    filtered_view.clearInvalidEntries(tid + BoolLessBatchOperator::tagStride());

    return filtered_view;
}

std::vector<int64_t> executeExistsClause(View &filtered_orders, View &filtered_lineitem, int tid) {
    auto orders_orderkey_col = filtered_orders._dataCols[0];
    auto lineitem_orderkey_col = filtered_lineitem._dataCols[0];

    auto exists_results = Views::in(orders_orderkey_col, lineitem_orderkey_col,
                                    filtered_orders._dataCols[filtered_orders.colNum() + View::VALID_COL_OFFSET],
                                    filtered_lineitem._dataCols[filtered_lineitem.colNum() + View::VALID_COL_OFFSET]);

    return exists_results;
}

View filterOrdersByExists(View &filtered_orders, std::vector<int64_t> &exists_results, int tid) {
    int vIdx = filtered_orders.colNum() + View::VALID_COL_OFFSET;
    auto new_valid = BoolAndBatchOperator(&filtered_orders._dataCols[vIdx], &exists_results, 1, 0,
                        tid, SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    filtered_orders._dataCols[vIdx] = std::move(new_valid);

    return filtered_orders;
}

View executeGroupByCount(View &final_orders, int tid) {
    if (final_orders.rowNum() == 0) {
        std::vector<std::string> result_fields = {"o_orderpriority", "order_count"};
        std::vector<int> result_widths = {64, 64};
        View result_view(result_fields, result_widths);
        return result_view;
    }

    std::string priority_field = "o_orderpriority";
    auto group_heads = final_orders.groupBy(priority_field, tid);

    std::vector<std::string> group_fields = {priority_field};
    final_orders.count(group_fields, group_heads, "order_count", tid);

    return final_orders;
}

View executeSortByPriority(View &result_view, int tid) {
    if (result_view.rowNum() == 0) {
        return result_view;
    }

    std::string priority_field = "o_orderpriority";
    result_view.sort(priority_field, true, tid);

    return result_view;
}
