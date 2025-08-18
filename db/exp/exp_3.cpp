//
// Created by 杜建璋 on 25-8-16.
//

/*
 * SELECT count(DISTINCT pid)
 * FROM diagnosis as d, medication as m
 * on d.pid = m.pid
 * WHERE d.diag = hd
 * AND m.med = aspirin
 * AND d.time <= m.time
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
#include "compute/batch/arith/ArithToBoolBatchOperator.h"
#include "parallel/ThreadPoolSupport.h"

#include <string>
#include <vector>
#include <random>
#include <algorithm>
#include <set>

// Forward declarations
void generateTestData(int diagRows, int medRows,
                      std::vector<int64_t> &diagnosis_pid_data,
                      std::vector<int64_t> &diagnosis_diag_data,
                      std::vector<int64_t> &diagnosis_time_data,
                      std::vector<int64_t> &diagnosis_tag_data,
                      std::vector<int64_t> &medication_pid_data,
                      std::vector<int64_t> &medication_med_data,
                      std::vector<int64_t> &medication_time_data,
                      std::vector<int64_t> &medication_tag_data);

View createDiagnosisTable(std::vector<int64_t> &diagnosis_pid_shares,
                          std::vector<int64_t> &diagnosis_diag_shares,
                          std::vector<int64_t> &diagnosis_time_shares,
                          std::vector<int64_t> &diagnosis_tag_shares);

View createMedicationTable(std::vector<int64_t> &medication_pid_shares,
                           std::vector<int64_t> &medication_med_shares,
                           std::vector<int64_t> &medication_time_shares,
                           std::vector<int64_t> &medication_tag_shares);

View filterDiagnosisTable(View &diagnosis_view, int64_t hd_value, int tid);

View filterMedicationTable(View &medication_view, int64_t aspirin_value, int tid);

View performJoin(View &filtered_diagnosis, View &filtered_medication, int tid);

View filterByTimeCondition(View &joined_view, int tid);

View executeDistinctCount(View &final_view, int tid);

void displayResults(View &result_view, int tid);

int main(int argc, char *argv[]) {
    System::init(argc, argv);
    auto tid = System::nextTask();

    // Read number of rows from command line
    int diagRows = 1000, medRows = 1000;
    if (Conf::_userParams.count("diagRows")) {
        diagRows = std::stoi(Conf::_userParams["diagRows"]);
    }
    if (Conf::_userParams.count("medRows")) {
        medRows = std::stoi(Conf::_userParams["medRows"]);
    }

    // Generate test data
    std::vector<int64_t> diagnosis_pid_data, diagnosis_diag_data, diagnosis_time_data, diagnosis_tag_data;
    std::vector<int64_t> medication_pid_data, medication_med_data, medication_time_data, medication_tag_data;
    generateTestData(diagRows, medRows,
                     diagnosis_pid_data, diagnosis_diag_data, diagnosis_time_data, diagnosis_tag_data,
                     medication_pid_data, medication_med_data, medication_time_data, medication_tag_data);

    // Convert to secret shares for 2PC
    auto diagnosis_pid_shares = Secrets::boolShare(diagnosis_pid_data, 2, 64, tid);
    auto diagnosis_diag_shares = Secrets::boolShare(diagnosis_diag_data, 2, 64, tid);
    auto diagnosis_time_shares = Secrets::boolShare(diagnosis_time_data, 2, 64, tid);
    auto diagnosis_tag_shares = Secrets::boolShare(diagnosis_tag_data, 2, 64, tid);

    auto medication_pid_shares = Secrets::boolShare(medication_pid_data, 2, 64, tid);
    auto medication_med_shares = Secrets::boolShare(medication_med_data, 2, 64, tid);
    auto medication_time_shares = Secrets::boolShare(medication_time_data, 2, 64, tid);
    auto medication_tag_shares = Secrets::boolShare(medication_tag_data, 2, 64, tid);

    View result_view;
    if (Comm::isServer()) {
        Log::i("Starting query execution...");
        auto query_start = System::currentTimeMillis();

        // Create tables
        auto diagnosis_view = createDiagnosisTable(diagnosis_pid_shares, diagnosis_diag_shares,
                                                   diagnosis_time_shares, diagnosis_tag_shares);
        auto medication_view = createMedicationTable(medication_pid_shares, medication_med_shares,
                                                      medication_time_shares, medication_tag_shares);

        // Execute query steps with optimization: filter before join
        int64_t hd_value = Comm::rank() * 100;      // Example value for 'hd'
        int64_t aspirin_value = Comm::rank() * 200; // Example value for 'aspirin'

        auto filtered_diagnosis = filterDiagnosisTable(diagnosis_view, hd_value, tid);
        auto filtered_medication = filterMedicationTable(medication_view, aspirin_value, tid);

        auto joined_view = performJoin(filtered_diagnosis, filtered_medication, tid);
        auto time_filtered_view = filterByTimeCondition(joined_view, tid);
        result_view = executeDistinctCount(time_filtered_view, tid);

        auto query_end = System::currentTimeMillis();
        Log::i("Total query execution time: {}ms", query_end - query_start);
    }

    // Display results
    displayResults(result_view, tid);

    System::finalize();
    return 0;
}

// Function implementations

void generateTestData(int diagRows, int medRows,
                      std::vector<int64_t> &diagnosis_pid_data,
                      std::vector<int64_t> &diagnosis_diag_data,
                      std::vector<int64_t> &diagnosis_time_data,
                      std::vector<int64_t> &diagnosis_tag_data,
                      std::vector<int64_t> &medication_pid_data,
                      std::vector<int64_t> &medication_med_data,
                      std::vector<int64_t> &medication_time_data,
                      std::vector<int64_t> &medication_tag_data) {
    if (Comm::rank() == 2) {
        // Generate diagnosis data
        diagnosis_pid_data.reserve(diagRows);
        diagnosis_diag_data.reserve(diagRows);
        diagnosis_time_data.reserve(diagRows);
        diagnosis_tag_data.reserve(diagRows);

        for (int i = 0; i < diagRows; i++) {
            int64_t pid = Math::randInt();
            int64_t diag = Math::randInt();
            int64_t time = Math::randInt();

            diagnosis_pid_data.push_back(pid);
            diagnosis_diag_data.push_back(diag);
            diagnosis_time_data.push_back(time);

            // Compute bucket tag using hash of the pid (join key)
            diagnosis_tag_data.push_back(Views::hash(pid));
        }

        // Generate medication data
        medication_pid_data.reserve(medRows);
        medication_med_data.reserve(medRows);
        medication_time_data.reserve(medRows);
        medication_tag_data.reserve(medRows);

        for (int i = 0; i < medRows; i++) {
            int64_t pid = Math::randInt();
            int64_t med = Math::randInt();
            int64_t time = Math::randInt();

            medication_pid_data.push_back(pid);
            medication_med_data.push_back(med);
            medication_time_data.push_back(time);

            // Compute bucket tag using hash of the pid (join key)
            medication_tag_data.push_back(Views::hash(pid));
        }

        Log::i("Generated {} diagnosis records and {} medication records", diagRows, medRows);
        Log::i("Random data - Full range random int64_t values for PIDs, diagnosis codes, medicine codes, and times");
        Log::i("Filter values - Alice: diag=0, med=0; Bob: diag=100, med=200");
    }
}

View createDiagnosisTable(std::vector<int64_t> &diagnosis_pid_shares,
                          std::vector<int64_t> &diagnosis_diag_shares,
                          std::vector<int64_t> &diagnosis_time_shares,
                          std::vector<int64_t> &diagnosis_tag_shares) {
    std::string diagnosis_name = "diagnosis";
    std::vector<std::string> diagnosis_fields = {"pid", "diag", "time"};
    std::vector<int> diagnosis_widths = {64, 64, 64};

    Table diagnosis_table(diagnosis_name, diagnosis_fields, diagnosis_widths, "pid");

    for (size_t i = 0; i < diagnosis_pid_shares.size(); i++) {
        std::vector<int64_t> row = {
            diagnosis_pid_shares[i],
            diagnosis_diag_shares[i],
            diagnosis_time_shares[i],
            diagnosis_tag_shares[i]  // bucket tag for pid
        };
        diagnosis_table.insert(row);
    }

    auto diagnosis_view = Views::selectAll(diagnosis_table);
    Log::i("Created diagnosis table with {} rows", diagnosis_table.rowNum());
    return diagnosis_view;
}

View createMedicationTable(std::vector<int64_t> &medication_pid_shares,
                           std::vector<int64_t> &medication_med_shares,
                           std::vector<int64_t> &medication_time_shares,
                           std::vector<int64_t> &medication_tag_shares) {
    std::string medication_name = "medication";
    std::vector<std::string> medication_fields = {"pid", "med", "time"};
    std::vector<int> medication_widths = {64, 64, 64};

    Table medication_table(medication_name, medication_fields, medication_widths, "pid");

    for (size_t i = 0; i < medication_pid_shares.size(); i++) {
        std::vector<int64_t> row = {
            medication_pid_shares[i],
            medication_med_shares[i],
            medication_time_shares[i],
            medication_tag_shares[i]  // bucket tag for pid
        };
        medication_table.insert(row);
    }

    auto medication_view = Views::selectAll(medication_table);
    Log::i("Created medication table with {} rows", medication_table.rowNum());
    return medication_view;
}

View filterDiagnosisTable(View &diagnosis_view, int64_t hd_value, int tid) {
    Log::i("Step 1: Filtering diagnosis table for diag = {}...", hd_value);
    auto step1_start = System::currentTimeMillis();

    std::vector<std::string> fieldNames = {"diag"};
    std::vector<View::ComparatorType> comparatorTypes = {View::EQUALS};
    std::vector<int64_t> constShares = {hd_value};

    View filtered_diagnosis = diagnosis_view;
    filtered_diagnosis.filterAndConditions(fieldNames, comparatorTypes, constShares);

    auto step1_end = System::currentTimeMillis();
    Log::i("Step 1 completed in {}ms", step1_end - step1_start);
    Log::i("Filtered diagnosis table has {} rows", filtered_diagnosis.rowNum());
    return filtered_diagnosis;
}

View filterMedicationTable(View &medication_view, int64_t aspirin_value, int tid) {
    Log::i("Step 2: Filtering medication table for med = {}...", aspirin_value);
    auto step2_start = System::currentTimeMillis();

    std::vector<std::string> fieldNames = {"med"};
    std::vector<View::ComparatorType> comparatorTypes = {View::EQUALS};
    std::vector<int64_t> constShares = {aspirin_value};


    View filtered_medication = medication_view;
    filtered_medication.filterAndConditions(fieldNames, comparatorTypes, constShares);

    auto step2_end = System::currentTimeMillis();
    Log::i("Step 2 completed in {}ms", step2_end - step2_start);
    Log::i("Filtered medication table has {} rows", filtered_medication.rowNum());
    return filtered_medication;
}

View performJoin(View &filtered_diagnosis, View &filtered_medication, int tid) {
    Log::i("Step 3: Performing JOIN on pid...");
    auto step3_start = System::currentTimeMillis();

    std::string join_field_d = "pid";
    std::string join_field_m = "pid";

    // Use hash join since we have bucket tags
    auto joined_view = Views::hashJoin(filtered_diagnosis, filtered_medication, join_field_d, join_field_m);

    auto step3_end = System::currentTimeMillis();
    Log::i("Step 3 completed in {}ms", step3_end - step3_start);
    Log::i("Joined table has {} rows", joined_view.rowNum());
    return joined_view;
}

View filterByTimeCondition(View &joined_view, int tid) {
    Log::i("Step 4: Filtering by time condition (d.time <= m.time)...");
    auto step4_start = System::currentTimeMillis();

    if (joined_view.rowNum() == 0) {
        auto step4_end = System::currentTimeMillis();
        Log::i("Step 4 completed in {}ms (empty input)", step4_end - step4_start);
        return joined_view;
    }

    // Find the time columns in the joined view
    // After join: diagnosis.pid, diagnosis.diag, diagnosis.time, medication.pid, medication.med, medication.time
    int diag_time_col = -1, med_time_col = -1;
    
    for (int i = 0; i < joined_view._fieldNames.size(); i++) {
        if (joined_view._fieldNames[i] == "time" || joined_view._fieldNames[i] == "diagnosis.time") {
            diag_time_col = i;
        } else if (joined_view._fieldNames[i] == "medication.time") {
            med_time_col = i;
        }
    }
    
    // If we can't find the exact field names, try by position
    if (diag_time_col == -1 || med_time_col == -1) {
        // Assume diagnosis fields come first, then medication fields
        // diagnosis: pid(0), diag(1), time(2), medication: pid(3), med(4), time(5)
        diag_time_col = 2;
        med_time_col = 5;
    }

    if (diag_time_col >= 0 && med_time_col >= 0 && 
        diag_time_col < joined_view._dataCols.size() && 
        med_time_col < joined_view._dataCols.size()) {
        
        auto &diag_time_data = joined_view._dataCols[diag_time_col];
        auto &med_time_data = joined_view._dataCols[med_time_col];
        
        // d.time <= m.time is equivalent to d.time < m.time OR d.time == m.time
        // We can use BoolLessBatchOperator for d.time <= m.time directly
        std::vector<int64_t> time_condition;
        
        if (Conf::BATCH_SIZE <= 0 || Conf::DISABLE_MULTI_THREAD) {
            time_condition = BoolLessBatchOperator(&diag_time_data, &med_time_data, 64, 0, tid,
                                                   SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
            // For <=, we need to flip the result (less gives us >, we want <=)
            for (auto &v : time_condition) {
                v = v ^ Comm::rank();
            }
        } else {
            size_t data_size = diag_time_data.size();
            int batchSize = Conf::BATCH_SIZE;
            int batchNum = (data_size + batchSize - 1) / batchSize;
            
            std::vector<std::future<std::vector<int64_t>>> batch_futures(batchNum);
            
            for (int b = 0; b < batchNum; ++b) {
                batch_futures[b] = ThreadPoolSupport::submit([&, b]() {
                    int start = b * batchSize;
                    int end = std::min(start + batchSize, static_cast<int>(data_size));
                    
                    std::vector<int64_t> batch_diag_time(diag_time_data.begin() + start, diag_time_data.begin() + end);
                    std::vector<int64_t> batch_med_time(med_time_data.begin() + start, med_time_data.begin() + end);
                    
                    auto batch_result = BoolLessBatchOperator(&batch_med_time, &batch_diag_time, 64, 0, 
                                                              tid + b * BoolLessBatchOperator::tagStride(),
                                                              SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
                    
                    return batch_result;
                });
            }
            
            time_condition.reserve(data_size);
            for (auto &f : batch_futures) {
                auto batch_res = f.get();
                time_condition.insert(time_condition.end(), batch_res.begin(), batch_res.end());
            }
        }
        
        // Apply the time condition filter
        joined_view._dataCols[joined_view.colNum() + View::VALID_COL_OFFSET] = time_condition;
        joined_view.clearInvalidEntries(tid);
    }

    auto step4_end = System::currentTimeMillis();
    Log::i("Step 4 completed in {}ms", step4_end - step4_start);
    Log::i("Time-filtered table has {} rows", joined_view.rowNum());
    return joined_view;
}

View executeDistinctCount(View &final_view, int tid) {
    Log::i("Step 5: Computing COUNT(DISTINCT pid)...");
    auto step5_start = System::currentTimeMillis();

    if (final_view.rowNum() == 0) {
        // Create empty result
        std::vector<std::string> result_fields = {"distinct_pid_count"};
        std::vector<int> result_widths = {64};
        View result_view(result_fields, result_widths);
        
        // Add a single row with count = 0
        result_view._dataCols[0].push_back(0);
        result_view._dataCols[result_view.colNum() + View::VALID_COL_OFFSET].push_back(Comm::rank());
        result_view._dataCols[result_view.colNum() + View::PADDING_COL_OFFSET].push_back(0);
        
        auto step5_end = System::currentTimeMillis();
        Log::i("Step 5 completed in {}ms (empty input)", step5_end - step5_start);
        return result_view;
    }

    // Extract pid column (should be the first column from diagnosis side)
    int pid_col_index = 0; // First column should be diagnosis.pid
    
    // Create a view with only the pid column for distinct operation
    std::vector<std::string> pid_fields = {"pid"};
    std::vector<int> pid_widths = {64};
    View pid_view(pid_fields, pid_widths);
    
    pid_view._dataCols[0] = final_view._dataCols[pid_col_index];
    pid_view._dataCols[pid_view.colNum() + View::VALID_COL_OFFSET] = std::vector<int64_t>(pid_view._dataCols[0].size(), Comm::rank()); // valid
    pid_view._dataCols[pid_view.colNum() + View::PADDING_COL_OFFSET] = std::vector<int64_t>(pid_view._dataCols[0].size(), 0); // padding
    // Apply DISTINCT operation
    pid_view.distinct(tid);
    
    // Count the distinct pids
    int64_t distinct_count = pid_view.rowNum();
    
    // Create result view
    std::vector<std::string> result_fields = {"distinct_pid_count"};
    std::vector<int> result_widths = {64};
    View result_view(result_fields, result_widths);
    
    // // Convert count to secret shares
    // std::vector<int64_t> count_data = {distinct_count};
    // auto count_shares = ArithToBoolBatchOperator(&count_data, 64, 0, tid,
    //                                              SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    
    result_view._dataCols[0].push_back(Comm::rank() * distinct_count);
    result_view._dataCols[result_view.colNum() + View::VALID_COL_OFFSET].push_back(Comm::rank());
    result_view._dataCols[result_view.colNum() + View::PADDING_COL_OFFSET].push_back(0);

    auto step5_end = System::currentTimeMillis();
    Log::i("Step 5 completed in {}ms", step5_end - step5_start);
    return result_view;
}

void displayResults(View &result_view, int tid) {
    Log::i("Reconstructing results for verification...");
    std::vector<int64_t> count_col;
    if (Comm::isServer() && !result_view._dataCols.empty()) {
        count_col = result_view._dataCols[0];
    }

    auto count_plain = Secrets::boolReconstruct(count_col, 2, 64, tid);

    if (Comm::rank() == 2) {
        Log::i("Query Results:");
        Log::i("COUNT(DISTINCT pid): {}", count_plain.empty() ? 0 : count_plain[0]);
    }
}
