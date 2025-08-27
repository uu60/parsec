//
// Created by 杜建璋 on 25-8-15.
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
#include <random>
#include <algorithm>
#include <set>

#include "utils/Math.h"
#include "compute/batch/bool/BoolToArithBatchOperator.h"
#include "compute/batch/arith/ArithMultiplyBatchOperator.h"
#include "compute/batch/arith/ArithAddBatchOperator.h"
#include "compute/batch/bool/BoolXorBatchOperator.h"
#include "compute/batch/bool/BoolAndBatchOperator.h"
#include "compute/batch/arith/ArithToBoolBatchOperator.h"
#include "compute/batch/arith/ArithLessBatchOperator.h"
#include "compute/batch/bool/BoolLessBatchOperator.h"

// Forward declarations
void generateTestData(int rows,
                      std::vector<int64_t> &id_data,
                      std::vector<int64_t> &pwd_data);

View createRTable(std::vector<int64_t> &id_shares,
                  std::vector<int64_t> &pwd_shares);

View executeGroupByCount(View &r_view, int tid);

std::vector<int64_t> executeHavingClause(View &counted, int tid);

View filterByHaving(View &grouped_view, std::vector<int64_t> &having_results, int tid);

/**
 * SELECT ID
 * FROM R
 * GROUP BY ID, PWD
 * HAVING COUNT(*)>1
 */
int main(int argc, char *argv[]) {
    System::init(argc, argv);
    auto tid = System::nextTask() << (32 - Conf::TASK_TAG_BITS);

    // Read number of rows from command line
    int rows = 1000;
    if (Conf::_userParams.count("rows")) {
        rows = std::stoi(Conf::_userParams["rows"]);
    }

    if (Comm::isClient()) {
        Log::i("Data size: R: {}", rows);
    }

    // Generate test data
    std::vector<int64_t> id_data, pwd_data;
    generateTestData(rows, id_data, pwd_data);

    // Convert to secret shares for 2PC
    auto id_shares = Secrets::boolShare(id_data, 2, 64, tid);
    auto pwd_shares = Secrets::boolShare(pwd_data, 2, 64, tid);

    View result_view;
    if (Comm::isServer()) {
        // Create table R (not counted in query execution time)
        auto r_view = createRTable(id_shares, pwd_shares);

        auto query_start = System::currentTimeMillis();

        // Execute query steps (only these are counted)
        auto counted_view = executeGroupByCount(r_view, tid);
        auto having_results = executeHavingClause(counted_view, tid);
        result_view = filterByHaving(counted_view, having_results, tid);

        auto query_end = System::currentTimeMillis();
        Log::i("Core query execution time: {}ms", query_end - query_start);
    }

    System::finalize();
    return 0;
}

// Function implementations

void generateTestData(int rows,
                      std::vector<int64_t> &id_data,
                      std::vector<int64_t> &pwd_data) {
    if (Comm::rank() == 2) {
        id_data.reserve(rows);
        pwd_data.reserve(rows);

        for (int i = 0; i < rows; i++) {
            id_data.push_back(Math::randInt());
            pwd_data.push_back(Math::randInt());
        }
    }
}

View createRTable(std::vector<int64_t> &id_shares,
                  std::vector<int64_t> &pwd_shares) {
    std::string table_name = "R";
    std::vector<std::string> fields = {"ID", "PWD"};
    std::vector<int> widths = {64, 64};

    Table r_table(table_name, fields, widths, "");

    for (size_t i = 0; i < id_shares.size(); i++) {
        std::vector<int64_t> row = {
            id_shares[i],
            pwd_shares[i]
        };
        r_table.insert(row);
    }

    auto r_view = Views::selectAll(r_table);
    return r_view;
}

View executeGroupByCount(View &r_view, int tid) {
    std::vector<std::string> group_fields = {"ID", "PWD"};
    auto group_heads = r_view.groupBy(group_fields, tid);

    size_t n = r_view.rowNum();
    if (n == 0) {
        std::vector<std::string> result_fields = {"ID", "PWD", "cnt"};
        std::vector<int> result_widths = {64, 64, 64};
        View result_view(result_fields, result_widths);
        return result_view;
    }

    r_view.count(group_fields, group_heads, "cnt", tid);

    return r_view;
}

std::vector<int64_t> executeHavingClause(View &counted, int tid) {
    // Get the count column
    auto count_col = counted._dataCols[counted.colIndex("cnt")];
    
    // Create a vector of 1s (constant for comparison)
    std::vector<int64_t> ones(count_col.size(), Comm::rank());
    
    // Compare count > 1 using ArithLessBatchOperator (1 < count)
    auto having_results = BoolLessBatchOperator(&ones, &count_col, 64, tid, 0,
                                                 SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

    return having_results;
}

View filterByHaving(View &grouped_view, std::vector<int64_t> &having_results, int tid) {
    // Create result view with only ID column (as specified in SELECT clause)
    std::vector<std::string> result_fields = {"ID"};
    std::vector<int> result_widths = {64};
    View result_view(result_fields, result_widths);
    
    // Copy ID column
    result_view._dataCols[result_view.colIndex("ID")] = grouped_view._dataCols[grouped_view.colIndex("ID")];
    
    // Set valid column to having results
    result_view._dataCols[result_view.colNum() + View::VALID_COL_OFFSET] = having_results;
    result_view._dataCols[result_view.colNum() + View::PADDING_COL_OFFSET] = std::vector<int64_t>(result_view.rowNum(), 0);

    result_view.clearInvalidEntries(tid);

    return result_view;
}