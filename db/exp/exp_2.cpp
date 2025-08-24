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
#include "compute/batch/bool/BoolLessBatchOperator.h"
#include "compute/batch/bool/BoolEqualBatchOperator.h"


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
 * 
 * OPTIMIZATION: Pre-calculate time+15, time+56, and row_no+1 as redundant data
 * to reduce arith/bool conversions during query execution
 */

#include "../include/basis/Table.h"
#include "conf/DbConf.h"
#include "parallel/ThreadPoolSupport.h"

// Forward declarations
void generateTestData(int num_records,
                      std::vector<int64_t> &pid_data,
                      std::vector<int64_t> &pid_hash_data,
                      std::vector<int64_t> &time_data,
                      std::vector<int64_t> &time_plus_15_data,
                      std::vector<int64_t> &time_plus_56_data,
                      std::vector<int64_t> &row_no_data,
                      std::vector<int64_t> &row_no_plus_1_data,
                      std::vector<int64_t> &diag_data);

View createDiagnosisTable(std::vector<int64_t> &pid_shares,
                          std::vector<int64_t> &pid_hash_shares,
                          std::vector<int64_t> &time_shares,
                          std::vector<int64_t> &time_plus_15_shares,
                          std::vector<int64_t> &time_plus_56_shares,
                          std::vector<int64_t> &row_no_shares,
                          std::vector<int64_t> &row_no_plus_1_shares,
                          std::vector<int64_t> &diag_shares);

View filterCdiffDiagnosis(View &diagnosis_view, int64_t cdiff_code, int tid);

View performSelfJoin(View &rcd_view, int tid);

View filterTimeAndRowConditions(View &joined_view, int tid);

View selectDistinctPid(View &filtered_view, int tid);

void displayResults(View &result_view, int tid);

int main(int argc, char *argv[]) {
    System::init(argc, argv);
    auto tid = System::nextTask() << (32 - Conf::TASK_TAG_BITS);

    // Read parameters from command line
    int num_records = 1000;
    int64_t cdiff_code = 123; // C. diff diagnosis code

    if (Conf::_userParams.count("num_records")) {
        num_records = std::stoi(Conf::_userParams["num_records"]);
    }
    if (Conf::_userParams.count("cdiff_code")) {
        cdiff_code = std::stoll(Conf::_userParams["cdiff_code"]);
    }

    // Generate test data with pre-calculated redundant fields
    std::vector<int64_t> pid_data, pid_hash_data, time_data, time_plus_15_data, time_plus_56_data;
    std::vector<int64_t> row_no_data, row_no_plus_1_data, diag_data;
    generateTestData(num_records, pid_data, pid_hash_data, time_data, time_plus_15_data, 
                     time_plus_56_data, row_no_data, row_no_plus_1_data, diag_data);

    // Convert to secret shares for 2PC
    auto pid_shares = Secrets::boolShare(pid_data, 2, 64, tid);
    auto pid_hash_shares = Secrets::boolShare(pid_hash_data, 2, 64, tid);
    auto time_shares = Secrets::boolShare(time_data, 2, 64, tid);
    auto time_plus_15_shares = Secrets::boolShare(time_plus_15_data, 2, 64, tid);
    auto time_plus_56_shares = Secrets::boolShare(time_plus_56_data, 2, 64, tid);
    auto row_no_shares = Secrets::boolShare(row_no_data, 2, 64, tid);
    auto row_no_plus_1_shares = Secrets::boolShare(row_no_plus_1_data, 2, 64, tid);
    auto diag_shares = Secrets::boolShare(diag_data, 2, 64, tid);

    View result_view;
    if (Comm::isServer()) {
        Log::i("Starting C. diff recurrence detection query with pre-calculated redundant data...");
        auto query_start = System::currentTimeMillis();

        // Step 1: Create diagnosis table with hash tags and redundant fields
        auto diagnosis_view = createDiagnosisTable(pid_shares, pid_hash_shares, time_shares, 
                                                   time_plus_15_shares, time_plus_56_shares,
                                                   row_no_shares, row_no_plus_1_shares, diag_shares);

        // Step 2: Filter for C. diff diagnoses (WITH rcd AS ...)
        auto rcd_view = filterCdiffDiagnosis(diagnosis_view, cdiff_code, tid);

        // Step 3: Perform self-join on pid
        auto joined_view = performSelfJoin(rcd_view, tid);

        // Step 4: Filter by time and row_no conditions using pre-calculated values
        auto filtered_view = filterTimeAndRowConditions(joined_view, tid);

        // Step 5: Select distinct pid
        result_view = selectDistinctPid(filtered_view, tid);

        auto query_end = System::currentTimeMillis();
        Log::i("Total query execution time: {}ms", query_end - query_start);
    }

    // Display results
    displayResults(result_view, tid);

    System::finalize();
    return 0;
}

// Function implementations

void generateTestData(int num_records,
                      std::vector<int64_t> &pid_data,
                      std::vector<int64_t> &pid_hash_data,
                      std::vector<int64_t> &time_data,
                      std::vector<int64_t> &time_plus_15_data,
                      std::vector<int64_t> &time_plus_56_data,
                      std::vector<int64_t> &row_no_data,
                      std::vector<int64_t> &row_no_plus_1_data,
                      std::vector<int64_t> &diag_data) {
    if (Comm::rank() == 2) {
        // Use fixed test data for verification
        // Patient 1: 3 diagnoses - C.diff (day 100), other (day 120), C.diff (day 130) - should detect recurrence
        // Patient 2: 2 diagnoses - C.diff (day 200), C.diff (day 230) - should detect recurrence (30 days apart)
        // Patient 3: 2 diagnoses - C.diff (day 300), C.diff (day 370) - should NOT detect (70 days apart, too long)
        // Patient 4: 2 diagnoses - C.diff (day 400), C.diff (day 410) - should NOT detect (10 days apart, too short)
        // Patient 5: 1 diagnosis - C.diff (day 500) - should NOT detect (only one diagnosis)
        // Patient 6: 2 diagnoses - other (day 600), other (day 620) - should NOT detect (no C.diff)
        
        pid_data = {1, 1, 1, 2, 2, 3, 3, 4, 4, 5, 6, 6};
        time_data = {100, 120, 130, 200, 230, 300, 370, 400, 410, 500, 600, 620};
        row_no_data = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
        diag_data = {123, 456, 123, 123, 123, 123, 123, 123, 123, 123, 456, 456};
        
        // Pre-calculate redundant data to avoid runtime arith/bool conversions
        time_plus_15_data.reserve(time_data.size());
        time_plus_56_data.reserve(time_data.size());
        row_no_plus_1_data.reserve(row_no_data.size());
        
        for (size_t i = 0; i < time_data.size(); i++) {
            time_plus_15_data.push_back(time_data[i] + 15);
            time_plus_56_data.push_back(time_data[i] + 56);
            row_no_plus_1_data.push_back(row_no_data[i] + 1);
        }
        
        // Calculate hash values for patient keys using Views::hash
        pid_hash_data.reserve(pid_data.size());
        for (int64_t key : pid_data) {
            pid_hash_data.push_back(Views::hash(key));
        }
        
        Log::i("Generated FIXED test data for C. diff recurrence detection with redundant fields:");
        Log::i("=================================================================================");
        Log::i("Total records: {}", pid_data.size());
        
        for (size_t i = 0; i < pid_data.size(); i++) {
            std::string diag_name = (diag_data[i] == 123) ? "C.diff" : "Other";
            Log::i("Record {}: Patient {}, Day {} (+15={}, +56={}), Row {} (+1={}), Diagnosis {} ({}), Hash {}", 
                   i+1, pid_data[i], time_data[i], time_plus_15_data[i], time_plus_56_data[i],
                   row_no_data[i], row_no_plus_1_data[i], diag_data[i], diag_name, pid_hash_data[i]);
        }
        
        Log::i("\nExpected C. diff recurrence analysis:");
        Log::i("=====================================");
        Log::i("Patient 1: C.diff (day 100, row 1) -> C.diff (day 130, row 3)");
        Log::i("  Time diff: 30 days (15-56 range: YES), Row diff: 3-1=2 (consecutive: NO)");
        Log::i("  Expected: NO recurrence (not consecutive rows)");
        
        Log::i("Patient 2: C.diff (day 200, row 4) -> C.diff (day 230, row 5)");
        Log::i("  Time diff: 30 days (15-56 range: YES), Row diff: 5-4=1 (consecutive: YES)");
        Log::i("  Expected: YES recurrence detected");
        
        Log::i("Patient 3: C.diff (day 300, row 6) -> C.diff (day 370, row 7)");
        Log::i("  Time diff: 70 days (15-56 range: NO), Row diff: 7-6=1 (consecutive: YES)");
        Log::i("  Expected: NO recurrence (time gap too long)");
        
        Log::i("Patient 4: C.diff (day 400, row 8) -> C.diff (day 410, row 9)");
        Log::i("  Time diff: 10 days (15-56 range: NO), Row diff: 9-8=1 (consecutive: YES)");
        Log::i("  Expected: NO recurrence (time gap too short)");
        
        Log::i("Patient 5: Only one C.diff diagnosis");
        Log::i("  Expected: NO recurrence (insufficient data)");
        
        Log::i("Patient 6: No C.diff diagnoses");
        Log::i("  Expected: NO recurrence (no C.diff)");
        
        Log::i("\nFinal Expected Result: Patient 2 should be detected with recurrence");
        Log::i("OPTIMIZATION: Pre-calculated time+15, time+56, and row_no+1 to reduce conversions");
        
        // Count C. diff diagnoses
        int cdiff_count = std::count(diag_data.begin(), diag_data.end(), 123);
        Log::i("C. diff diagnoses: {}/{} records", cdiff_count, pid_data.size());
    }
}

View createDiagnosisTable(std::vector<int64_t> &pid_shares,
                          std::vector<int64_t> &pid_hash_shares,
                          std::vector<int64_t> &time_shares,
                          std::vector<int64_t> &time_plus_15_shares,
                          std::vector<int64_t> &time_plus_56_shares,
                          std::vector<int64_t> &row_no_shares,
                          std::vector<int64_t> &row_no_plus_1_shares,
                          std::vector<int64_t> &diag_shares) {
    std::string table_name = "diagnosis";
    std::vector<std::string> fields = {"pid", "time", "time_plus_15", "time_plus_56", 
                                       "row_no", "row_no_plus_1", "diag"};
    std::vector<int> widths = {64, 64, 64, 64, 64, 64, 64};

    // Create table with pid as primary key to automatically add hash tag column
    Table diagnosis_table(table_name, fields, widths, "pid");

    for (size_t i = 0; i < pid_shares.size(); i++) {
        std::vector<int64_t> row = {
            pid_shares[i],
            time_shares[i],
            time_plus_15_shares[i],
            time_plus_56_shares[i],
            row_no_shares[i],
            row_no_plus_1_shares[i],
            diag_shares[i],
            pid_hash_shares[i], // Hash tag column
        };
        diagnosis_table.insert(row);
    }

    auto diagnosis_view = Views::selectAll(diagnosis_table);
    Log::i("Created diagnosis table with {} rows and redundant fields", diagnosis_table.rowNum());
    Log::i("Table includes hash tag column for primary key 'pid'");
    Log::i("Redundant fields: time_plus_15, time_plus_56, row_no_plus_1");
    return diagnosis_view;
}

View filterCdiffDiagnosis(View &diagnosis_view, int64_t cdiff_code, int tid) {
    Log::i("Step 1: Filtering for C. diff diagnoses (diag = {})...", cdiff_code);
    auto step1_start = System::currentTimeMillis();

    std::vector<std::string> fieldNames = {"diag"};
    std::vector<View::ComparatorType> comparatorTypes = {View::EQUALS};
    std::vector<int64_t> constShares = {Comm::rank() * cdiff_code};

    View rcd_view = diagnosis_view;
    rcd_view.filterAndConditions(fieldNames, comparatorTypes, constShares, tid);

    auto step1_end = System::currentTimeMillis();
    Log::i("Step 1 completed in {}ms", step1_end - step1_start);
    Log::i("C. diff records: {} rows", rcd_view.rowNum());
    Views::revealAndPrint(rcd_view);
    return rcd_view;
}

View performSelfJoin(View &rcd_view, int tid) {
    Log::i("Step 2: Performing self-join on pid...");
    auto step2_start = System::currentTimeMillis();

    // Use hash-based join with the bucket tag column
    std::string left_key = "pid";
    std::string right_key = "pid";
    auto rcd_view_copy = rcd_view; // Create a copy to avoid modifying the original view
    rcd_view._tableName = "r1";
    rcd_view_copy._tableName = "r2";
    auto joined_view = Views::hashJoin(rcd_view, rcd_view_copy, left_key, right_key);

    auto step2_end = System::currentTimeMillis();
    Log::i("Step 2 completed in {}ms", step2_end - step2_start);
    Log::i("Joined table has {} rows", joined_view.rowNum());
    Views::revealAndPrint(joined_view);
    return joined_view;
}

View filterTimeAndRowConditions(View &joined_view, int tid) {
    Log::i("Step 3: Filtering by time and row_no conditions using pre-calculated values...");
    auto step3_start = System::currentTimeMillis();

    if (joined_view.rowNum() == 0) {
        auto step3_end = System::currentTimeMillis();
        Log::i("Step 3 completed in {}ms (empty input)", step3_end - step3_start);
        return joined_view;
    }

    // Get column indices for the joined table using pre-calculated redundant fields
    int r1_time_plus_15_idx = joined_view.colIndex("r1.time_plus_15");
    int r1_time_plus_56_idx = joined_view.colIndex("r1.time_plus_56");
    int r2_time_idx = joined_view.colIndex("r2.time");
    int r1_row_no_plus_1_idx = joined_view.colIndex("r1.row_no_plus_1");
    int r2_row_no_idx = joined_view.colIndex("r2.row_no");

    if (r1_time_plus_15_idx < 0 || r1_time_plus_56_idx < 0 || r2_time_idx < 0 || 
        r1_row_no_plus_1_idx < 0 || r2_row_no_idx < 0) {
        Log::e("Required columns not found in joined view");
        auto step3_end = System::currentTimeMillis();
        Log::i("Step 3 completed in {}ms (column error)", step3_end - step3_start);
        return joined_view;
    }

    size_t num_rows = joined_view.rowNum();

    // Get column data directly as boolean shares - no arith/bool conversions needed
    auto &r1_time_plus_15_col = joined_view._dataCols[r1_time_plus_15_idx];
    auto &r1_time_plus_56_col = joined_view._dataCols[r1_time_plus_56_idx];
    auto &r2_time_col = joined_view._dataCols[r2_time_idx];
    auto &r1_row_no_plus_1_col = joined_view._dataCols[r1_row_no_plus_1_idx];
    auto &r2_row_no_col = joined_view._dataCols[r2_row_no_idx];

    // Step 1: Check if r2.time >= r1.time + 15 (equivalent to r2.time >= r1.time_plus_15)
    // Using BoolLess: r1.time_plus_15 <= r2.time is equivalent to NOT(r2.time < r1.time_plus_15)
    auto time_ge_15 = BoolLessBatchOperator(&r1_time_plus_15_col, &r2_time_col, 64, 0, tid,
                                            SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

    // Step 2: Check if r2.time <= r1.time + 56 (equivalent to r2.time <= r1.time_plus_56)
    // Using BoolLess: r2.time <= r1.time_plus_56 is equivalent to NOT(r1.time_plus_56 < r2.time)
    auto time_le_56 = BoolLessBatchOperator(&r2_time_col, &r1_time_plus_56_col, 64, 0, tid,
                                            SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

    // Step 3: Combine time conditions (r1.time_plus_15 <= r2.time <= r1.time_plus_56)
    auto time_in_range = BoolAndBatchOperator(&time_ge_15, &time_le_56, 1, 0, tid,
                                              SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

    // Step 4: Check if r2.row_no = r1.row_no + 1 (equivalent to r2.row_no = r1.row_no_plus_1)
    auto row_consecutive = BoolEqualBatchOperator(&r2_row_no_col, &r1_row_no_plus_1_col, 64, 0, tid,
                                                  SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

    // Step 5: Combine all conditions (time_in_range AND row_consecutive)
    auto final_condition = BoolAndBatchOperator(&time_in_range, &row_consecutive, 1, 0, tid,
                                                SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

    // Step 6: Apply the filter condition to the view
    View filtered_view = joined_view;
    int valid_col_idx = filtered_view.colNum() + View::VALID_COL_OFFSET;
    filtered_view._dataCols[valid_col_idx] = final_condition;
    filtered_view.clearInvalidEntries(tid);

    auto step3_end = System::currentTimeMillis();
    Log::i("Step 3 completed in {}ms", step3_end - step3_start);
    Log::i("Applied optimized filtering using pre-calculated redundant fields");
    Log::i("Avoided runtime calculations: time+15, time+56, row_no+1");
    Log::i("Used direct BoolLess and BoolEqual operations - no arith/bool conversions");
    Log::i("Filtered table has {} rows", filtered_view.rowNum());
    return filtered_view;
}

View selectDistinctPid(View &filtered_view, int tid) {
    Log::i("Step 4: Selecting distinct pid...");
    auto step4_start = System::currentTimeMillis();

    if (filtered_view.rowNum() == 0) {
        std::vector<std::string> result_fields = {"pid"};
        std::vector<int> result_widths = {64};
        View result_view(result_fields, result_widths);

        auto step4_end = System::currentTimeMillis();
        Log::i("Step 4 completed in {}ms (empty input)", step4_end - step4_start);
        return result_view;
    }

    std::vector<std::string> field_names = {"r1.pid"};
    filtered_view.select(field_names);
    // Apply distinct operation on all columns
    filtered_view.distinct(tid);

    auto step4_end = System::currentTimeMillis();
    Log::i("Step 4 completed in {}ms", step4_end - step4_start);
    Log::i("Distinct result has {} rows", filtered_view.rowNum());
    return filtered_view;
}

void displayResults(View &result_view, int tid) {
    Log::i("Reconstructing results for verification...");
    std::vector<int64_t> pid_col;
    if (Comm::isServer() && !result_view._dataCols.empty()) {
        // Find pid column (should be the first non-hash column)
        int pid_idx = result_view.colIndex("r1.pid");
        if (pid_idx >= 0) {
            pid_col = result_view._dataCols[pid_idx];
        }
    }

    auto pid_plain = Secrets::boolReconstruct(pid_col, 2, 64, tid);

    if (Comm::rank() == 2) {
        Log::i("C. diff Recurrence Detection Results (Optimized with Redundant Data):");
        Log::i("Patients with recurring C. diff infections:");
        Log::i("========================================");

        for (size_t i = 0; i < pid_plain.size(); i++) {
            Log::i("Patient ID: {}", pid_plain[i]);
        }

        Log::i("\nQuery Summary:");
        Log::i("Found {} patients with C. diff recurrence", pid_plain.size());
        Log::i("Criteria: Second infection 15-56 days after first infection");
        Log::i("Sequential diagnosis requirement: row_no2 = row_no + 1");
        Log::i("Used hash-based optimization for join operations");
        Log::i("OPTIMIZATION: Pre-calculated time+15, time+56, row_no+1 to reduce arith/bool conversions");
    }
}
