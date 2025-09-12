//
// Created by 杜建璋 on 25-8-1.
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
#include "conf/DbConf.h"

// Forward declarations
void generateTestData(int rows1, int rows2,
                      std::vector<int64_t> &diagnosis_pid_data,
                      std::vector<int64_t> &diagnosis_diag_data,
                      std::vector<int64_t> &diagnosis_extra_data,
                      std::vector<int64_t> &cdiff_cohort_pid_data);

View createDiagnosisTable(std::vector<int64_t> &diagnosis_pid_shares,
                          std::vector<int64_t> &diagnosis_diag_shares,
                          std::vector<int64_t> &diagnosis_extra_shares);

View createCohortTable(std::vector<int64_t> &cdiff_cohort_pid_shares);

std::vector<int64_t> executeWhereInClause(View &diagnosis_view, View &cdiff_cohort_view);

View filterDiagnosisTable(View &diagnosis_view, std::vector<int64_t> &in_results, int tid);

View executeGroupByCount(View &filtered_diagnosis, int tid);

View executeSortAndLimit(View result_view, int tid);

/**
 * SELECT diag, COUNT(*) cnt
 * FROM diagnosis
 * WHERE pid IN cdiff_cohort
 * GROUP BY diag
 * ORDER BY cnt DESC
 * LIMIT 10
 */
int main(int argc, char *argv[]) {
    System::init(argc, argv);
    DbConf::init();
    auto tid = System::nextTask() << (32 - Conf::TASK_TAG_BITS);

    // Read number of rows from command line
    int rows1 = 1000, rows2 = 1000;
    if (Conf::_userParams.count("rows1")) {
        rows1 = std::stoi(Conf::_userParams["rows1"]);
    }
    if (Conf::_userParams.count("rows2")) {
        rows2 = std::stoi(Conf::_userParams["rows2"]);
    }

    if (Comm::isClient()) {
        Log::i("Data size: diagnosis: {} cdiff_cohort: {}", rows1, rows2);
    }

    // Generate test data
    std::vector<int64_t> diagnosis_pid_data, diagnosis_diag_data, diagnosis_extra_data;
    std::vector<int64_t> cdiff_cohort_pid_data;
    generateTestData(rows1, rows2, diagnosis_pid_data, diagnosis_diag_data,
                     diagnosis_extra_data, cdiff_cohort_pid_data);

    // Convert to secret shares for 2PC
    auto diagnosis_pid_shares = Secrets::boolShare(diagnosis_pid_data, 2, 64, tid);
    auto diagnosis_diag_shares = Secrets::boolShare(diagnosis_diag_data, 2, 64, tid);
    auto diagnosis_extra_shares = Secrets::boolShare(diagnosis_extra_data, 2, 64, tid);
    auto cdiff_cohort_pid_shares = Secrets::boolShare(diagnosis_pid_data, 2, 64, tid);

    View result_view;
    if (Comm::isServer()) {
        auto query_start = System::currentTimeMillis();

        // Create tables
        auto diagnosis_view = createDiagnosisTable(diagnosis_pid_shares, diagnosis_diag_shares, diagnosis_extra_shares);
        auto cdiff_cohort_view = createCohortTable(cdiff_cohort_pid_shares);

        // Execute query steps
        auto in_results = executeWhereInClause(diagnosis_view, cdiff_cohort_view);
        auto filtered_diagnosis = filterDiagnosisTable(diagnosis_view, in_results, tid);
        result_view = executeGroupByCount(filtered_diagnosis, tid);
        result_view = executeSortAndLimit(std::move(result_view), tid);

        auto query_end = System::currentTimeMillis();
        Log::i("Total query execution time: {}ms", query_end - query_start);
    }

    System::finalize();
    return 0;
}

// Function implementations

void generateTestData(int rows1, int rows2,
                      std::vector<int64_t> &diagnosis_pid_data,
                      std::vector<int64_t> &diagnosis_diag_data,
                      std::vector<int64_t> &diagnosis_extra_data,
                      std::vector<int64_t> &cdiff_cohort_pid_data) {
    if (Comm::rank() == 2) {
        diagnosis_pid_data.reserve(rows1);
        diagnosis_diag_data.reserve(rows1);
        diagnosis_extra_data.reserve(rows1);

        for (int i = 0; i < rows1; i++) {
            diagnosis_pid_data.push_back(Math::randInt());
            diagnosis_diag_data.push_back(Math::randInt());
            diagnosis_extra_data.push_back(Math::randInt()); // Just some extra data
        }

        cdiff_cohort_pid_data.reserve(rows2);
        for (int i = 0; i < rows2; i++) {
            cdiff_cohort_pid_data.push_back(Math::randInt());
        }
    }
}

View createDiagnosisTable(std::vector<int64_t> &diagnosis_pid_shares,
                          std::vector<int64_t> &diagnosis_diag_shares,
                          std::vector<int64_t> &diagnosis_extra_shares) {
    std::string diagnosis_name = "diagnosis";
    std::vector<std::string> diagnosis_fields = {"pid", "diag", "extra_field"};
    std::vector<int> diagnosis_widths = {64, 64, 64};

    Table diagnosis_table(diagnosis_name, diagnosis_fields, diagnosis_widths, "");

    for (size_t i = 0; i < diagnosis_pid_shares.size(); i++) {
        std::vector<int64_t> row = {
            diagnosis_pid_shares[i],
            diagnosis_diag_shares[i],
            diagnosis_extra_shares[i]
        };
        diagnosis_table.insert(row);
    }

    auto diagnosis_view = Views::selectAll(diagnosis_table);
    return diagnosis_view;
}

View createCohortTable(std::vector<int64_t> &cdiff_cohort_pid_shares) {
    std::string cohort_name = "cdiff_cohort";
    std::vector<std::string> cohort_fields = {"pid"};
    std::vector<int> cohort_widths = {64};

    Table cdiff_cohort_table(cohort_name, cohort_fields, cohort_widths, "");

    for (size_t i = 0; i < cdiff_cohort_pid_shares.size(); i++) {
        std::vector<int64_t> row = {cdiff_cohort_pid_shares[i]};
        cdiff_cohort_table.insert(row);
    }

    auto cdiff_cohort_view = Views::selectAll(cdiff_cohort_table);
    return cdiff_cohort_view;
}

std::vector<int64_t> executeWhereInClause(View &diagnosis_view, View &cdiff_cohort_view) {
    auto diagnosis_pid_col = diagnosis_view._dataCols[0];
    auto cdiff_cohort_pid_col = cdiff_cohort_view._dataCols[0];

    auto in_results = Views::in(diagnosis_pid_col, cdiff_cohort_pid_col,
                                diagnosis_view._dataCols[diagnosis_view.colNum() + View::VALID_COL_OFFSET],
                                cdiff_cohort_view._dataCols[cdiff_cohort_view.colNum() + View::VALID_COL_OFFSET]);

    return in_results;
}

View filterDiagnosisTable(View &diagnosis_view, std::vector<int64_t> &in_results, int tid) {
    View filtered_diagnosis = diagnosis_view;
    filtered_diagnosis._dataCols[filtered_diagnosis.colNum() + View::VALID_COL_OFFSET] = in_results;
    filtered_diagnosis.clearInvalidEntries(tid);

    return filtered_diagnosis;
}

View executeGroupByCount(View &filtered_diagnosis, int tid) {
    std::string diag_field = "diag";
    auto group_heads = filtered_diagnosis.groupBy(diag_field, tid);

    size_t n = filtered_diagnosis.rowNum();
    if (n == 0) {
        std::vector<std::string> result_fields = {"diag", "cnt"};
        std::vector<int> result_widths = {64, 64};
        View result_view(result_fields, result_widths);
        return result_view;
    }

    std::vector<std::string> group_fields = {diag_field};
    filtered_diagnosis.count(group_fields, group_heads, "cnt", tid);

    return filtered_diagnosis;
}

View executeSortAndLimit(View result_view, int tid) {
    std::string count_field = "cnt";
    result_view.sort(count_field, false, tid); // false for descending order

    int limit = std::min(10, (int) result_view.rowNum());
    if (limit < result_view.rowNum()) {
        // Truncate to first 10 rows
        for (int col = 0; col < result_view.colNum(); col++) {
            result_view._dataCols[col].resize(limit);
        }
    }

    return result_view;
}
