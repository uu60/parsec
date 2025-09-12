//
// Q2 correctness test (fixed data + expected output)
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
#include <algorithm>
#include <future>
#include <map>
#include <set>

#include "utils/Math.h"
#include "compute/batch/bool/BoolAndBatchOperator.h"
#include "compute/batch/bool/BoolLessBatchOperator.h"
#include "compute/batch/bool/BoolEqualBatchOperator.h"

#include "../include/basis/Table.h"
#include "conf/DbConf.h"
#include "parallel/ThreadPoolSupport.h"

/**
 * Target SQL:
 * WITH rcd AS (
 *     SELECT pid, time, row_no
 *     FROM diagnosis WHERE diag=cdiff
 * )
 * SELECT DISTINCT pid
 * FROM rcd r1 JOIN rcd r2
 *   ON r1.pid = r2.pid
 * WHERE r2.time - r1.time >= 15
 *   AND r2.time - r1.time <= 56
 *   AND r2.row_no = r1.row_no + 1
 *
 * Optimized physical plan (no join):
 *   sort(pid,time) -> filter diag=cdiff -> compaction (rcd)
 *   -> adjacent compare within rcd (i with i+1) for window & same pid
 *   -> project(pid) -> distinct
 *
 * This file replaces random data with a FIXED dataset and prints the expected pids.
 */

// -------------------- Fixed test data --------------------
static constexpr int64_t CDIFF_CODE = 1001;

// Each element is one diagnosis row (pid, time, diag)
static const std::vector<std::tuple<int64_t,int64_t,int64_t>> FIXED_ROWS = {
    // pid 1: cdiff at t=10,30,90; (30-10)=20 in [15,56] => HIT
    {1, 10, CDIFF_CODE},
    {1, 20, 2002},
    {1, 30, CDIFF_CODE},
    {1, 90, CDIFF_CODE},

    // pid 2: cdiff at t=0,14,70; (14-0)=14 (fail), (70-14)=56 (HIT)
    {2, 0,  CDIFF_CODE},
    {2, 14, CDIFF_CODE},
    {2, 70, CDIFF_CODE},

    // pid 3: only one cdiff -> no hit
    {3, 50, 3003},
    {3, 80, CDIFF_CODE},

    // pid 4: cdiff at t=100,120,160; (120-100)=20 (HIT)
    {4, 100, CDIFF_CODE},
    {4, 120, CDIFF_CODE},
    {4, 160, CDIFF_CODE},

    // pid 5: cdiff at t=5,80; (80-5)=75 (fail)
    {5, 5,  CDIFF_CODE},
    {5, 80, CDIFF_CODE},

    // pid 6: cdiff at t=100,115; (115-100)=15 (HIT, boundary)
    {6, 100, CDIFF_CODE},
    {6, 115, CDIFF_CODE},
};

// EXPECTED DISTINCT PIDs (as per SQL semantics): {1, 2, 4, 6}
static const std::set<int64_t> EXPECTED_PIDS = {1, 2, 4, 6};

// -------------------- Forward declarations --------------------
void generateFixedTestData(std::vector<int64_t> &pid_data,
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

// Optimized pipeline funcs
View buildRcdNoJoin(View diagnosis_view, int64_t cdiff_code, int tid);
View markAdjacentPairsWithinPid(View rcd_view, int tid);
View selectDistinctPid(View &filtered_view, int tid);

// Helper to print expected PIDs (client side)
void printExpected();

// -------------------- Main --------------------
int main(int argc, char *argv[]) {
    System::init(argc, argv);
    DbConf::init();
    auto tid = System::nextTask() << (32 - Conf::TASK_TAG_BITS);

    if (Comm::isClient()) {
        Log::i("[TEST] Using FIXED dataset with {} rows", (int)FIXED_ROWS.size());
        printExpected();
    }

    // Build fixed dataset
    std::vector<int64_t> pid_data, pid_hash_data, time_data, time_plus_15_data, time_plus_56_data;
    std::vector<int64_t> row_no_data, row_no_plus_1_data, diag_data;
    generateFixedTestData(pid_data, pid_hash_data, time_data, time_plus_15_data,
                          time_plus_56_data, row_no_data, row_no_plus_1_data, diag_data);

    // Convert to secret shares for 2PC
    // Note: We share the fixed plaintext rows so both servers get their boolean shares.
    auto tid_share = tid;
    auto pid_shares           = Secrets::boolShare(pid_data,            2, 64, tid_share);
    auto pid_hash_shares      = Secrets::boolShare(pid_hash_data,       2, 64, tid_share);
    auto time_shares          = Secrets::boolShare(time_data,           2, 64, tid_share);
    auto time_plus_15_shares  = Secrets::boolShare(time_plus_15_data,   2, 64, tid_share);
    auto time_plus_56_shares  = Secrets::boolShare(time_plus_56_data,   2, 64, tid_share);
    auto row_no_shares        = Secrets::boolShare(row_no_data,         2, 64, tid_share);
    auto row_no_plus_1_shares = Secrets::boolShare(row_no_plus_1_data,  2, 64, tid_share);
    auto diag_shares          = Secrets::boolShare(diag_data,           2, 64, tid_share);

    // We use a PUBLIC constant for cdiff code in this correctness test.
    // If your filter requires secret shares for constants, adapt to send per-party shares here.
    const int64_t cdiff_code = CDIFF_CODE * Comm::rank();

    if (Comm::isServer()) {
        auto query_start = System::currentTimeMillis();

        // Step 1: Create diagnosis table with hash tag & redundant fields
        auto diagnosis_view = createDiagnosisTable(pid_shares, pid_hash_shares, time_shares,
                                                   time_plus_15_shares, time_plus_56_shares,
                                                   row_no_shares, row_no_plus_1_shares, diag_shares);

        // Step 2–3: rcd = sort(pid,time) + filter(diag=cdiff) + compaction
        auto rcd_view = buildRcdNoJoin(diagnosis_view, cdiff_code, tid);

        // Step 4: adjacent compare within rcd (i with i+1) for time window & same pid
        auto paired_view = markAdjacentPairsWithinPid(rcd_view, tid);

        // Step 5: select distinct pid
        auto result_view = selectDistinctPid(paired_view, tid);

        Views::revealAndPrint(result_view);

        auto query_end = System::currentTimeMillis();
        Log::i("[TEST] Exec time={}ms | rows_in={} rows_rcd={} rows_hits={}",
               (query_end - query_start),
               diagnosis_view.rowNum(), rcd_view.rowNum(), result_view.rowNum());

        // Optional: if you have an "open" primitive to reveal a column, use it here.
        // For example (pseudo):
        // auto pids = Views::openColumn(result_view, "pid");
        // Log::i("[TEST] Actual PIDs: {}", fmt::join(pids, ","));
        // Then manually compare with EXPECTED_PIDS.
    }

    System::finalize();
    return 0;
}

// -------------------- Function implementations --------------------

void generateFixedTestData(std::vector<int64_t> &pid_data,
                           std::vector<int64_t> &pid_hash_data,
                           std::vector<int64_t> &time_data,
                           std::vector<int64_t> &time_plus_15_data,
                           std::vector<int64_t> &time_plus_56_data,
                           std::vector<int64_t> &row_no_data,
                           std::vector<int64_t> &row_no_plus_1_data,
                           std::vector<int64_t> &diag_data) {
    // Only client materializes plaintext rows
    if (Comm::rank() == 2) {
        const int N = (int)FIXED_ROWS.size();
        pid_data.reserve(N);
        time_data.reserve(N);
        diag_data.reserve(N);
        row_no_data.reserve(N);
        row_no_plus_1_data.reserve(N);
        time_plus_15_data.reserve(N);
        time_plus_56_data.reserve(N);

        for (int i = 0; i < N; ++i) {
            auto [pid, t, d] = FIXED_ROWS[i];
            pid_data.push_back(pid);
            time_data.push_back(t);
            diag_data.push_back(d);
            row_no_data.push_back(i + 1);       // not used by the optimized plan
            row_no_plus_1_data.push_back(i + 2); // not used by the optimized plan

            time_plus_15_data.push_back(t + 15);
            time_plus_56_data.push_back(t + 56);
        }

        // Hash tags for pid (plaintext hashing on client)
        pid_hash_data.reserve(N);
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

// ---- Step 2–3: build rcd without join ----
View buildRcdNoJoin(View diagnosis_view, int64_t cdiff_code, int tid) {
    // Sort by pid, time (ascending)
    // std::vector<std::string> order = {"pid", "time"};
    // std::vector<bool> asc = {true, true};
    // diagnosis_view.sort(order, asc, tid);

    // Filter diag == cdiff (here we pass a PUBLIC constant for test convenience)
    std::vector<std::string> fieldNames = {"diag"};
    std::vector<View::ComparatorType> comparatorTypes = {View::EQUALS};
    std::vector<int64_t> constShares = {cdiff_code};
    diagnosis_view.filterAndConditions(fieldNames, comparatorTypes, constShares, false, tid);

    // Stable compaction: keep only selected rows (rcd)
    // const int clearTid = tid + 64;
    // diagnosis_view.clearInvalidEntries(clearTid);
    std::vector<std::string> order1 = {"pid", "$valid", "time"};
    std::vector<bool> asc1 = {true, false, true};
    diagnosis_view.sort(order1, asc1, tid);
    return diagnosis_view;
}

// ---- Step 4: adjacent compare within rcd (i with i+1) ----
// ---- Optimized Step 4: adjacent compare within rcd (i with i+1) ----
// Step 4: adjacent compare (i with i+1) using only </== and XOR/AND (no NOT)
View markAdjacentPairsWithinPid(View rcd_view, int tid) {
    const int n_rows = rcd_view.rowNum();
    if (n_rows <= 1) return rcd_view;

    const int pid_idx   = rcd_view.colIndex("pid");
    const int t_idx     = rcd_view.colIndex("time");
    const int t15_idx   = rcd_view.colIndex("time_plus_15");
    const int t56_idx   = rcd_view.colIndex("time_plus_56");
    const int valid_idx = rcd_view.colNum() + View::VALID_COL_OFFSET; // $valid
    if (pid_idx < 0 || t_idx < 0 || t15_idx < 0 || t56_idx < 0) return rcd_view;

    auto &pid   = rcd_view._dataCols[pid_idx];
    auto &t     = rcd_view._dataCols[t_idx];
    auto &t15   = rcd_view._dataCols[t15_idx];
    auto &t56   = rcd_view._dataCols[t56_idx];
    auto &valid = rcd_view._dataCols[valid_idx];

    const size_t n = (size_t)n_rows;
    std::vector<int64_t> final_cond(n, 0);  // 命中标记，按 i 写入

    const int B = (Conf::BATCH_SIZE > 0 ? Conf::BATCH_SIZE : (int)n);
    const int batches = (int)((n + B - 1) / B);

    const int strideLess64 = BoolLessBatchOperator::tagStride();
    const int strideEq64   = BoolEqualBatchOperator::tagStride();
    const int strideAnd1   = BoolAndBatchOperator::tagStride();
    const int stridePerBatch = 2*strideLess64 + 3*strideEq64 + 5*strideAnd1;

    auto workBatch = [&](int b) -> std::pair<int,std::vector<int64_t>> {
        const int start = b * B;
        // i 取值区间：[start, endExclusive)；最后一个 i 是 endExclusive-1
        const int endExclusive = std::min(start + B, (int)n - 1);
        if (start >= endExclusive) return {start, {}};

        const int L = endExclusive - start;  // 本批输出长度（对应 i 的个数）

        // 切片：i 与 i+1
        std::vector<int64_t> pid_i (pid.begin()   + start, pid.begin()   + endExclusive);
        std::vector<int64_t> pid_j (pid.begin()   + start + 1, pid.begin()   + endExclusive + 1);
        std::vector<int64_t> t_i   (t.begin()     + start, t.begin()     + endExclusive);
        std::vector<int64_t> t_j   (t.begin()     + start + 1, t.begin()     + endExclusive + 1);
        std::vector<int64_t> t15_i (t15.begin()   + start, t15.begin()   + endExclusive);
        std::vector<int64_t> t56_i (t56.begin()   + start, t56.begin()   + endExclusive);
        std::vector<int64_t> v_i   (valid.begin() + start, valid.begin() + endExclusive);
        std::vector<int64_t> v_j   (valid.begin() + start + 1, valid.begin() + endExclusive + 1);

        int bt = tid + b * stridePerBatch;

        // ge15: (t15_i < t_j) OR (t15_i == t_j)
        auto lt_t15 = BoolLessBatchOperator(&t15_i, &t_j, 64, 0, bt,
                          SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis; bt += strideLess64;
        auto eq_t15 = BoolEqualBatchOperator(&t15_i, &t_j, 64, 0, bt,
                          SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis; bt += strideEq64;
        auto ltANDt15 = BoolAndBatchOperator(&lt_t15, &eq_t15, 1, 0, bt,
                          SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis; bt += strideAnd1;
        std::vector<int64_t> ge15 = lt_t15;            // OR = XOR + AND
        for (int k=0;k<L;++k) ge15[k] ^= eq_t15[k];
        for (int k=0;k<L;++k) ge15[k] ^= ltANDt15[k];

        // le56: (t_j < t56_i) OR (t_j == t56_i)
        auto lt_56 = BoolLessBatchOperator(&t_j, &t56_i, 64, 0, bt,
                          SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis; bt += strideLess64;
        auto eq_56 = BoolEqualBatchOperator(&t_j, &t56_i, 64, 0, bt,
                          SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis; bt += strideEq64;
        auto ltAND56 = BoolAndBatchOperator(&lt_56, &eq_56, 1, 0, bt,
                          SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis; bt += strideAnd1;
        std::vector<int64_t> le56 = lt_56;
        for (int k=0;k<L;++k) le56[k] ^= eq_56[k];
        for (int k=0;k<L;++k) le56[k] ^= ltAND56[k];

        // in-range = ge15 & le56
        auto inrng = BoolAndBatchOperator(&ge15, &le56, 1, 0, bt,
                          SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis; bt += strideAnd1;

        // same pid
        auto samepid = BoolEqualBatchOperator(&pid_i, &pid_j, 64, 0, bt,
                          SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis; bt += strideEq64;

        // both valid
        auto bothv = BoolAndBatchOperator(&v_i, &v_j, 1, 0, bt,
                          SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis; bt += strideAnd1;

        // final = inrng & samepid & bothv
        auto hit1 = BoolAndBatchOperator(&inrng, &samepid, 1, 0, bt,
                          SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis; bt += strideAnd1;
        auto hit  = BoolAndBatchOperator(&hit1, &bothv, 1, 0, bt,
                          SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        // 这里直接返回长度为 L 的结果（不再 +1）
        return {start, hit};
    };

    if (Conf::BATCH_SIZE <= 0 || Conf::DISABLE_MULTI_THREAD) {
        auto pr = workBatch(0);
        const int start = pr.first;
        auto &part = pr.second;               // 长度 n-1
        if (!part.empty()) {
            std::copy(part.begin(), part.end(), final_cond.begin() + start);
        }
    } else {
        std::vector<std::future<std::pair<int,std::vector<int64_t>>>> futs(batches);
        for (int b = 0; b < batches; ++b)
            futs[b] = ThreadPoolSupport::submit([&, b]() { return workBatch(b); });

        for (int b = 0; b < batches; ++b) {
            auto pr = futs[b].get();
            const int start = pr.first;
            auto &part = pr.second;           // 长度 endExclusive-start
            if (!part.empty()) {
                // 直接写入 final_cond 的对应区间
                std::copy(part.begin(), part.end(), final_cond.begin() + start);
            }
        }
    }

    // 最后一个位置 i = n-1 必为 0（因为没有 j = i+1）
    final_cond[n-1] = 0;

    View v = rcd_view;
    const int vidx = v.colNum() + View::VALID_COL_OFFSET;
    v._dataCols[vidx] = std::move(final_cond);

    v.clearInvalidEntries(tid + batches * stridePerBatch + 64); // 可选：便于调试
    return v;
}

// ---- Step 5: select distinct pid ----
View selectDistinctPid(View &filtered_view, int tid) {
    if (filtered_view.rowNum() == 0) {
        std::vector<std::string> result_fields = {"pid"};
        std::vector<int> result_widths = {64};
        View result_view(result_fields, result_widths);
        return result_view;
    }
    std::vector<std::string> field_names = {"pid"};
    filtered_view.select(field_names);
    Views::revealAndPrint(filtered_view);
    filtered_view.distinct(tid);
    return filtered_view;
}

// ---- Print expected output on client ----
void printExpected() {
    std::string s = "{";
    bool first = true;
    for (auto p : EXPECTED_PIDS) {
        if (!first) s += ", ";
        s += std::to_string(p);
        first = false;
    }
    s += "}";
    Log::i("[TEST] EXPECTED DISTINCT PIDs = {}", s);
}