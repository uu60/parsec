//
// Created by 杜建璋 on 25-8-14.
//

#include "secret/Secrets.h"
#include "utils/System.h"

#include "../include/basis/View.h"
#include "../include/operator/SelectSupport.h"
#include "basis/Views.h"
#include "utils/Log.h"

#include <string>
#include <vector>
#include <random>
#include <algorithm>
#include <future>

#include "utils/Math.h"
#include "compute/batch/bool/BoolAndBatchOperator.h"
#include "compute/batch/bool/BoolLessBatchOperator.h"
#include "compute/batch/bool/BoolEqualBatchOperator.h"

/**
 * Target SQL:
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
 * Optimized physical plan (no join):
 *   sort(diagnosis by pid,time) -> select diag=cdiff -> compaction(rcd)
 *   -> adjacent compare within rcd (i with i+1) for time window & same pid
 *   -> select pid -> distinct
 */

#include "../include/basis/Table.h"
#include "conf/DbConf.h"
#include "parallel/ThreadPoolSupport.h"

// -------------------- Forward declarations --------------------
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

// New pipeline funcs
View buildRcdNoJoin(View diagnosis_view, int64_t cdiff_code, int tid);
View markAdjacentPairsWithinPid(View rcd_view, int tid);
View selectDistinctPid(View &filtered_view, int tid);

// -------------------- Main --------------------
int main(int argc, char *argv[]) {
    System::init(argc, argv);
    DbConf::init();
    auto tid = System::nextTask() << (32 - Conf::TASK_TAG_BITS);

    // Read parameters from command line
    int num_records = 1000;
    if (Conf::_userParams.count("rows")) {
        num_records = std::stoi(Conf::_userParams["rows"]);
    }

    if (Comm::isClient()) {
        Log::i("Data size: diagnosis: {}", num_records);
    }

    // Generate test data with pre-calculated redundant fields
    std::vector<int64_t> pid_data, pid_hash_data, time_data, time_plus_15_data, time_plus_56_data;
    std::vector<int64_t> row_no_data, row_no_plus_1_data, diag_data;
    generateTestData(num_records, pid_data, pid_hash_data, time_data, time_plus_15_data,
                     time_plus_56_data, row_no_data, row_no_plus_1_data, diag_data);

    // Convert to secret shares for 2PC
    auto pid_shares           = Secrets::boolShare(pid_data,            2, 64, tid);
    auto pid_hash_shares      = Secrets::boolShare(pid_hash_data,       2, 64, tid);
    auto time_shares          = Secrets::boolShare(time_data,           2, 64, tid);
    auto time_plus_15_shares  = Secrets::boolShare(time_plus_15_data,   2, 64, tid);
    auto time_plus_56_shares  = Secrets::boolShare(time_plus_56_data,   2, 64, tid);
    auto row_no_shares        = Secrets::boolShare(row_no_data,         2, 64, tid);
    auto row_no_plus_1_shares = Secrets::boolShare(row_no_plus_1_data,  2, 64, tid);
    auto diag_shares          = Secrets::boolShare(diag_data,           2, 64, tid);

    // Secret-shared C.diff code (client sends, servers receive)
    int64_t cdiff_code;
    if (Comm::isClient()) {
        int64_t cdiff_code0 = Math::randInt();
        int64_t cdiff_code1 = Math::randInt();
        Comm::send(cdiff_code0, 64, 0, tid);
        Comm::send(cdiff_code1, 64, 1, tid);
    } else {
        Comm::receive(cdiff_code, 64, 2, tid);
    }

    if (Comm::isServer()) {
        auto query_start = System::currentTimeMillis();

        // Step 1: Create diagnosis table with hash tag & redundant fields
        auto diagnosis_view = createDiagnosisTable(pid_shares, pid_hash_shares, time_shares,
                                                   time_plus_15_shares, time_plus_56_shares,
                                                   row_no_shares, row_no_plus_1_shares, diag_shares);

        // Step 2–3 (optimized): rcd = sort(pid,time) + filter(diag=cdiff) + compaction
        auto rcd_view = buildRcdNoJoin(diagnosis_view, cdiff_code, tid);

        // Step 4 (optimized): adjacent compare within rcd (i with i+1) for time window & same pid
        auto paired_view = markAdjacentPairsWithinPid(rcd_view, tid);

        // Step 5: select distinct pid
        auto result_view = selectDistinctPid(paired_view, tid);

        auto query_end = System::currentTimeMillis();
        Log::i("Total query execution time: {}ms | rows_in={} rows_rcd={} rows_hits={}",
               (query_end - query_start),
               diagnosis_view.rowNum(), rcd_view.rowNum(), result_view.rowNum());
    }

    System::finalize();
    return 0;
}

// -------------------- Function implementations --------------------

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
        // Generate random test data for performance testing
        pid_data.reserve(num_records);
        time_data.reserve(num_records);
        row_no_data.reserve(num_records);
        diag_data.reserve(num_records);
        time_plus_15_data.reserve(num_records);
        time_plus_56_data.reserve(num_records);
        row_no_plus_1_data.reserve(num_records);

        for (int i = 0; i < num_records; i++) {
            pid_data.push_back(Math::randInt());
            time_data.push_back(Math::randInt());
            row_no_data.push_back(i + 1);
            diag_data.push_back(Math::randInt());

            // Pre-calculate redundant data
            time_plus_15_data.push_back(time_data[i] + 15);
            time_plus_56_data.push_back(time_data[i] + 56);
            row_no_plus_1_data.push_back(i + 2);
        }

        // Hash tags (plaintext generation on client)
        pid_hash_data.reserve(pid_data.size());
        for (int64_t key : pid_data) {
            pid_hash_data.push_back(Views::hash(key));
        }
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
    std::vector<std::string> fields = {
        "pid", "time", "time_plus_15", "time_plus_56",
        "row_no", "row_no_plus_1", "diag"
    };
    std::vector<int> widths = {64, 64, 64, 64, 64, 64, 64};

    // Create table with pid as primary key to automatically add hash tag column
    Table diagnosis_table(table_name, fields, widths, "pid");

    const size_t n = pid_shares.size();
    for (size_t i = 0; i < n; i++) {
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
    return diagnosis_view;
}

// ---- Optimized Step 2–3: build rcd without join ----
View buildRcdNoJoin(View diagnosis_view, int64_t cdiff_code, int tid) {
    // Sort by pid, time (ascending)
    std::vector<std::string> order = {"pid", "time"};
    std::vector<bool> asc = {true, true};
    diagnosis_view.sort(order, asc, tid);

    // Filter diag == cdiff
    std::vector<std::string> fieldNames = {"diag"};
    std::vector<View::ComparatorType> comparatorTypes = {View::EQUALS};
    std::vector<int64_t> constShares = {cdiff_code};
    diagnosis_view.filterAndConditions(fieldNames, comparatorTypes, constShares, tid);

    // Stable compaction: keep only selected rows (rcd)
    // Choose a clearTid beyond used tags in this stage
    const int clearTid = tid + 64;
    diagnosis_view.clearInvalidEntries(clearTid);
    return diagnosis_view;
}

// ---- Optimized Step 4: adjacent compare within rcd (i with i+1) ----
View markAdjacentPairsWithinPid(View rcd_view, int tid) {
    const int n_rows = rcd_view.rowNum();
    if (n_rows <= 1) return rcd_view;

    const int pid_idx  = rcd_view.colIndex("pid");
    const int t_idx    = rcd_view.colIndex("time");
    const int t15_idx  = rcd_view.colIndex("time_plus_15");
    const int t56_idx  = rcd_view.colIndex("time_plus_56");
    if (pid_idx < 0 || t_idx < 0 || t15_idx < 0 || t56_idx < 0) return rcd_view;

    auto &pid  = rcd_view._dataCols[pid_idx];
    auto &t    = rcd_view._dataCols[t_idx];
    auto &t15  = rcd_view._dataCols[t15_idx];
    auto &t56  = rcd_view._dataCols[t56_idx];

    const size_t n = (size_t)n_rows;
    std::vector<int64_t> final_cond(n, 0);  // keep i (r1) if i & i+1 hit

    // Batch settings
    const int B = (Conf::BATCH_SIZE > 0 ? Conf::BATCH_SIZE : (int)n);
    const int batches = (int)((n + B - 1) / B);

    // Tag stride accounting (conservative upper bound)
    const int strideLess64 = BoolLessBatchOperator::tagStride();
    const int strideAnd1   = BoolAndBatchOperator::tagStride();
    const int strideEq64   = BoolEqualBatchOperator::tagStride();
    const int stridePerBatch = strideLess64 + strideLess64 + strideAnd1 + strideEq64 + strideAnd1;

    auto workBatch = [&](int b) -> std::vector<int64_t> {
        const int start = b * B;
        const int endExclusive = std::min(start + B, (int)n - 1); // up to n-2 (i with i+1)
        if (start >= endExclusive) return {};

        // Slice i and i+1
        std::vector<int64_t> pid_i (pid.begin() + start, pid.begin() + endExclusive);
        std::vector<int64_t> pid_j (pid.begin() + start + 1, pid.begin() + endExclusive + 1);
        std::vector<int64_t> t_i   (t.begin()   + start, t.begin()   + endExclusive);
        std::vector<int64_t> t_j   (t.begin()   + start + 1, t.begin()   + endExclusive + 1);
        std::vector<int64_t> t15_i (t15.begin() + start, t15.begin() + endExclusive);
        std::vector<int64_t> t56_i (t56.begin() + start, t56.begin() + endExclusive);

        int bt = tid + b * stridePerBatch;

        // ge15: t_j >= t15_i <=> !(t_j < t15_i)
        auto lt15 = BoolLessBatchOperator(&t_j, &t15_i, 64, 0, bt,
                        SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis; bt += strideLess64;
        for (auto &x : lt15) x ^= 1; // not_lt15

        // le56: t_j <= t56_i <=> !(t56_i < t_j)
        auto gt56 = BoolLessBatchOperator(&t56_i, &t_j, 64, 0, bt,
                        SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis; bt += strideLess64;
        // in range: ge15 & le56
        auto inrng = BoolAndBatchOperator(&lt15, &gt56, 1, 0, bt,
                        SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis; bt += strideAnd1;

        // same pid: pid_i == pid_j
        auto samepid = BoolEqualBatchOperator(&pid_i, &pid_j, 64, 0, bt,
                        SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis; bt += strideEq64;

        // final = inrng & samepid
        auto hit = BoolAndBatchOperator(&inrng, &samepid, 1, 0, bt,
                        SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        // Write back into [start, endExclusive), and pad one zero for alignment
        std::vector<int64_t> ret(endExclusive - start + 1, 0);
        std::copy(hit.begin(), hit.end(), ret.begin());
        return ret;
    };

    if (Conf::BATCH_SIZE <= 0 || Conf::DISABLE_MULTI_THREAD) {
        auto part = workBatch(0);
        std::copy(part.begin(), part.end(), final_cond.begin());
    } else {
        std::vector<std::future<std::vector<int64_t>>> futs(batches);
        for (int b = 0; b < batches; ++b)
            futs[b] = ThreadPoolSupport::submit([&, b]() { return workBatch(b); });

        // Stitch
        size_t offset = 0;
        for (int b = 0; b < batches; ++b) {
            auto part = futs[b].get();
            if (part.empty()) continue;
            std::copy(part.begin(), part.end(), final_cond.begin() + offset);
            offset += part.size();
        }
    }

    // Apply selection bits to VALID column, keep i rows only
    View v = rcd_view;
    const int valid_idx = v.colNum() + View::VALID_COL_OFFSET;
    v._dataCols[valid_idx] = final_cond;

    const int clearTid = tid + batches * stridePerBatch + 64;
    v.clearInvalidEntries(clearTid);
    return v;
}

// ---- Step 5: select distinct pid (generic) ----
View selectDistinctPid(View &filtered_view, int tid) {
    if (filtered_view.rowNum() == 0) {
        std::vector<std::string> result_fields = {"pid"};
        std::vector<int> result_widths = {64};
        View result_view(result_fields, result_widths);
        return result_view;
    }
    // Project only pid to minimize work
    std::vector<std::string> field_names = {"pid"};
    filtered_view.select(field_names);
    // Use existing distinct (may sort internally depending on impl)
    filtered_view.distinct(tid);
    return filtered_view;
}