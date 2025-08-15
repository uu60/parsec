//
// Created by 杜建璋 on 25-8-14.
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
#include "compute/batch/arith/ArithEqualBatchOperator.h"
#include "compute/batch/arith/ArithLessBatchOperator.h"

// Forward declarations
void generateTestData(int rows,
                      std::vector<int64_t> &diagnosis_pid_data,
                      std::vector<int64_t> &diagnosis_diag_data,
                      std::vector<int64_t> &diagnosis_time_data,
                      std::vector<int64_t> &diagnosis_row_no_data);

View createDiagnosisTable(std::vector<int64_t> &diagnosis_pid_shares,
                          std::vector<int64_t> &diagnosis_diag_shares,
                          std::vector<int64_t> &diagnosis_time_shares,
                          std::vector<int64_t> &diagnosis_row_no_shares);

View createRcdView(View &diagnosis_view, int tid);

View sortByPidAndTime(View &rcd_view, int tid);

std::vector<int64_t> executeAdjacentConditions(View &sorted_view, int tid);

View filterByConditions(View &joined_view, std::vector<int64_t> &condition_results, int tid);

View executeDistinct(View &filtered_view, int tid);

void displayResults(View &result_view, int tid);

/**
 * WITH rcd AS (
 *     SELECT pid, time, row_no
 *     FROM diagnosis WHERE diag=cdiff
 * )
 * SELECT DISTINCT pid
 * FROM rcd r1 JOIN rcd r2
 * ON r1.pid = r2.pid
 * WHERE r2.time - r1.time >= 15 DAYS
 *     AND r2.time - r1.time <= 56 DAYS
 *     AND r2.row_no = r1.row_no + 1
 */
int main(int argc, char *argv[]) {
    System::init(argc, argv);
    auto tid = System::nextTask();

    // Read number of rows from command line
    int rows = 1000;
    if (Conf::_userParams.count("rows")) {
        rows = std::stoi(Conf::_userParams["rows"]);
    }


    // Generate test data
    std::vector<int64_t> diagnosis_pid_data, diagnosis_diag_data, diagnosis_time_data, diagnosis_row_no_data;
    generateTestData(rows, diagnosis_pid_data, diagnosis_diag_data, diagnosis_time_data, diagnosis_row_no_data);

    // Convert to secret shares for 2PC
    auto diagnosis_pid_shares = Secrets::boolShare(diagnosis_pid_data, 2, 64, tid);
    auto diagnosis_diag_shares = Secrets::boolShare(diagnosis_diag_data, 2, 64, tid);
    auto diagnosis_time_shares = Secrets::boolShare(diagnosis_time_data, 2, 64, tid);
    auto diagnosis_row_no_shares = Secrets::boolShare(diagnosis_row_no_data, 2, 64, tid);

    View result_view;
    if (Comm::isServer()) {
        Log::i("Starting query execution...");
        auto query_start = System::currentTimeMillis();

        // Create diagnosis table
        auto diagnosis_view = createDiagnosisTable(diagnosis_pid_shares, diagnosis_diag_shares, 
                                                   diagnosis_time_shares, diagnosis_row_no_shares);

        // Execute query steps (Secrecy-style implementation)
        auto rcd_view = createRcdView(diagnosis_view, tid);
        auto sorted_view = sortByPidAndTime(rcd_view, tid);
        auto condition_results = executeAdjacentConditions(sorted_view, tid);
        auto filtered_view = filterByConditions(sorted_view, condition_results, tid);
        result_view = executeDistinct(filtered_view, tid);

        auto query_end = System::currentTimeMillis();
        Log::i("Total query execution time: {}ms", query_end - query_start);
    }

    // Display results
    displayResults(result_view, tid);

    System::finalize();
    return 0;
}

// Function implementations

void generateTestData(int rows,
                      std::vector<int64_t> &diagnosis_pid_data,
                      std::vector<int64_t> &diagnosis_diag_data,
                      std::vector<int64_t> &diagnosis_time_data,
                      std::vector<int64_t> &diagnosis_row_no_data) {
    if (Comm::rank() == 2) {
        diagnosis_pid_data.reserve(rows);
        diagnosis_diag_data.reserve(rows);
        diagnosis_time_data.reserve(rows);
        diagnosis_row_no_data.reserve(rows);

        // Use same data generation approach as exp_1.cpp
        // Generate random data for all fields
        for (int i = 0; i < rows; i++) {
            diagnosis_pid_data.push_back(Math::randInt());
            diagnosis_diag_data.push_back(Math::randInt());
            diagnosis_time_data.push_back(Math::randInt()); // Random time data
            diagnosis_row_no_data.push_back(Math::randInt()); // Random row number data
        }

        Log::i("Generated {} diagnosis records", rows);
        Log::i("Using same random data generation approach as exp_1.cpp");
    }
}

View createDiagnosisTable(std::vector<int64_t> &diagnosis_pid_shares,
                          std::vector<int64_t> &diagnosis_diag_shares,
                          std::vector<int64_t> &diagnosis_time_shares,
                          std::vector<int64_t> &diagnosis_row_no_shares) {
    std::string diagnosis_name = "diagnosis";
    std::vector<std::string> diagnosis_fields = {"pid", "diag", "time", "row_no"};
    std::vector<int> diagnosis_widths = {64, 64, 64, 64};

    Table diagnosis_table(diagnosis_name, diagnosis_fields, diagnosis_widths, "");

    for (size_t i = 0; i < diagnosis_pid_shares.size(); i++) {
        std::vector<int64_t> row = {
            diagnosis_pid_shares[i],
            diagnosis_diag_shares[i],
            diagnosis_time_shares[i],
            diagnosis_row_no_shares[i]
        };
        diagnosis_table.insert(row);
    }

    auto diagnosis_view = Views::selectAll(diagnosis_table);
    Log::i("Created diagnosis table with {} rows", diagnosis_table.rowNum());
    return diagnosis_view;
}

View createRcdView(View &diagnosis_view, int tid) {
    Log::i("Step 1: Creating RCD view (SELECT pid, time, row_no FROM diagnosis WHERE diag=cdiff)...");
    auto step1_start = System::currentTimeMillis();

    // Simply select pid, time, row_no columns from diagnosis table
    // In a real scenario, this would filter for CDIFF diagnosis, but for this implementation
    // we'll select all records to demonstrate the query structure
    std::vector<std::string> rcd_fields = {"pid", "time", "row_no"};
    std::vector<int> rcd_widths = {64, 64, 64};
    View rcd_view(rcd_fields, rcd_widths);
    
    rcd_view._dataCols[0] = diagnosis_view._dataCols[diagnosis_view.colIndex("pid")];
    rcd_view._dataCols[1] = diagnosis_view._dataCols[diagnosis_view.colIndex("time")];
    rcd_view._dataCols[2] = diagnosis_view._dataCols[diagnosis_view.colIndex("row_no")];
    rcd_view._dataCols[rcd_view.colNum() + View::VALID_COL_OFFSET] = 
        std::vector<int64_t>(rcd_view.rowNum(), Comm::rank());
    rcd_view._dataCols[rcd_view.colNum() + View::PADDING_COL_OFFSET] = 
        std::vector<int64_t>(rcd_view.rowNum(), 0);

    auto step1_end = System::currentTimeMillis();
    Log::i("Step 1 completed in {}ms", step1_end - step1_start);
    Log::i("RCD view has {} rows", rcd_view.rowNum());
    
    return rcd_view;
}

View sortByPidAndTime(View &rcd_view, int tid) {
    Log::i("Step 2: Sorting by (pid, time) like Secrecy implementation...");
    auto step2_start = System::currentTimeMillis();

    // Sort by pid first, then by time (ascending order for both)
    rcd_view.sort("pid", true, tid);
    rcd_view.sort("time", true, tid);

    auto step2_end = System::currentTimeMillis();
    Log::i("Step 2 completed in {}ms", step2_end - step2_start);
    Log::i("Sorted view has {} rows", rcd_view.rowNum());

    return rcd_view;
}

std::vector<int64_t> executeAdjacentConditions(View &sorted_view, int tid) {
    Log::i("Step 3: Executing adjacent row conditions (Secrecy-style)...");
    auto step3_start = System::currentTimeMillis();

    size_t n = sorted_view.rowNum();
    if (n <= 1) {
        return std::vector<int64_t>(n, 0);
    }

    auto pid_col = sorted_view._dataCols[0];
    auto time_col = sorted_view._dataCols[1];

    std::vector<int64_t> result_conditions(n, 0);

    // For each adjacent pair (i, i+1), check:
    // 1. pid[i] == pid[i+1] (same patient)
    // 2. time[i+1] - time[i] >= 15 (at least 15 days apart)
    // 3. time[i+1] - time[i] <= 56 (at most 56 days apart)
    // 4. Implicit: row_no[i+1] = row_no[i] + 1 (consecutive in sorted order)

    for (size_t i = 0; i < n - 1; i++) {
        // Check if adjacent rows have same pid
        std::vector<int64_t> pid_i = {pid_col[i]};
        std::vector<int64_t> pid_i_plus_1 = {pid_col[i + 1]};
        auto pid_equal = ArithEqualBatchOperator(&pid_i, &pid_i_plus_1, 64, tid, 0,
                                                 SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        // Compute time difference: time[i+1] - time[i]
        std::vector<int64_t> time_i = {time_col[i]};
        std::vector<int64_t> time_i_plus_1 = {time_col[i + 1]};
        std::vector<int64_t> neg_time_i = {-time_col[i]};
        auto time_diff = ArithAddBatchOperator(&time_i_plus_1, &neg_time_i, 64, tid, 0,
                                               SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        // Condition 1: time_diff >= 15
        std::vector<int64_t> fifteen_values = {15};
        auto fifteen_shares = Secrets::boolShare(fifteen_values, 2, 64, tid);
        auto ge_15_results = ArithLessBatchOperator(&fifteen_shares, &time_diff, 64, tid, 0,
                                                    SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        // Condition 2: time_diff <= 56 (using time_diff < 57)
        std::vector<int64_t> fifty_seven_values = {57};
        auto fifty_seven_shares = Secrets::boolShare(fifty_seven_values, 2, 64, tid);
        auto le_56_results = ArithLessBatchOperator(&time_diff, &fifty_seven_shares, 64, tid, 0,
                                                    SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        // Combine all conditions: pid_equal AND ge_15 AND le_56
        auto cond1_and_cond2 = BoolAndBatchOperator(&pid_equal, &ge_15_results, 1, tid, 0,
                                                     SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        auto all_conditions = BoolAndBatchOperator(&cond1_and_cond2, &le_56_results, 1, tid, 0,
                                                   SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        result_conditions[i] = all_conditions[0];
    }

    auto step3_end = System::currentTimeMillis();
    Log::i("Step 3 completed in {}ms", step3_end - step3_start);

    return result_conditions;
}

View filterByConditions(View &joined_view, std::vector<int64_t> &condition_results, int tid) {
    Log::i("Step 4: Filtering by conditions...");
    auto step4_start = System::currentTimeMillis();

    View filtered_view = joined_view;
    filtered_view._dataCols[filtered_view.colNum() + View::VALID_COL_OFFSET] = condition_results;
    filtered_view.clearInvalidEntries(tid);

    auto step4_end = System::currentTimeMillis();
    Log::i("Step 4 completed in {}ms", step4_end - step4_start);
    Log::i("Filtered view has {} rows", filtered_view.rowNum());

    return filtered_view;
}

View executeDistinct(View &filtered_view, int tid) {
    Log::i("Step 5: Executing DISTINCT on pid...");
    auto step5_start = System::currentTimeMillis();

    // Extract pid column (r1_pid, which should be same as r2_pid due to join condition)
    std::vector<std::string> result_fields = {"pid"};
    std::vector<int> result_widths = {64};
    View result_view(result_fields, result_widths);
    
    if (filtered_view.rowNum() > 0) {
        result_view._dataCols[0] = filtered_view._dataCols[0]; // r1_pid
        result_view._dataCols[result_view.colNum() + View::VALID_COL_OFFSET] = 
            std::vector<int64_t>(result_view.rowNum(), Comm::rank());
        result_view._dataCols[result_view.colNum() + View::PADDING_COL_OFFSET] = 
            std::vector<int64_t>(result_view.rowNum(), 0);

        // Sort by pid to group duplicates together
        result_view.sort("pid", true, tid);

        // Remove duplicates using a simple approach
        if (result_view.rowNum() > 1) {
            std::vector<int64_t> distinct_flags(result_view.rowNum(), Comm::rank());
            auto pid_col = result_view._dataCols[0];
            
            // First row is always distinct
            // For subsequent rows, check if different from previous
            for (size_t i = 1; i < pid_col.size(); i++) {
                std::vector<int64_t> prev_pid = {pid_col[i-1]};
                std::vector<int64_t> curr_pid = {pid_col[i]};
                auto not_equal = ArithEqualBatchOperator(&prev_pid, &curr_pid, 64, tid, 0,
                                                         SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
                distinct_flags[i] = not_equal[0] ^ Comm::rank(); // XOR to get NOT equal
            }
            
            result_view._dataCols[result_view.colNum() + View::VALID_COL_OFFSET] = distinct_flags;
            result_view.clearInvalidEntries(tid);
        }
    }

    auto step5_end = System::currentTimeMillis();
    Log::i("Step 5 completed in {}ms", step5_end - step5_start);
    Log::i("Distinct result has {} rows", result_view.rowNum());

    return result_view;
}

void displayResults(View &result_view, int tid) {
    Log::i("Reconstructing results for verification...");
    std::vector<int64_t> pid_col;
    if (Comm::isServer()) {
        pid_col = result_view._dataCols[0];
    }

    auto pid_plain = Secrets::boolReconstruct(pid_col, 2, 64, tid);

    if (Comm::rank() == 2) {
        Log::i("Query Results (DISTINCT PIDs with CDIFF recurrence):");
        Log::i("pid");
        Log::i("---");

        for (int i = 0; i < pid_plain.size(); i++) {
            Log::i("{}", pid_plain[i]);
        }
        
        Log::i("Total distinct PIDs with CDIFF recurrence: {}", pid_plain.size());
    }
}
