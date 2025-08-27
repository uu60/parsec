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

#include "utils/Math.h"
#include "compute/batch/bool/BoolAndBatchOperator.h"
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

int main(int argc, char *argv[]) {
    System::init(argc, argv);
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
    auto pid_shares = Secrets::boolShare(pid_data, 2, 64, tid);
    auto pid_hash_shares = Secrets::boolShare(pid_hash_data, 2, 64, tid);
    auto time_shares = Secrets::boolShare(time_data, 2, 64, tid);
    auto time_plus_15_shares = Secrets::boolShare(time_plus_15_data, 2, 64, tid);
    auto time_plus_56_shares = Secrets::boolShare(time_plus_56_data, 2, 64, tid);
    auto row_no_shares = Secrets::boolShare(row_no_data, 2, 64, tid);
    auto row_no_plus_1_shares = Secrets::boolShare(row_no_plus_1_data, 2, 64, tid);
    auto diag_shares = Secrets::boolShare(diag_data, 2, 64, tid);

    int64_t cdiff_code; // C. diff diagnosis code
    if (Comm::isClient()) {
        int64_t cdiff_code0 = Math::randInt();
        int64_t cdiff_code1 = Math::randInt();
        Comm::send(cdiff_code0, 64, 0, tid);
        Comm::send(cdiff_code1, 64, 1, tid);
    } else {
        Comm::receive(cdiff_code, 64, 2, tid);
    }

    View result_view;
    if (Comm::isServer()) {
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
        // Generate random test data for performance testing
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

        // Calculate hash values
        for (int64_t key: pid_data) {
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
    return diagnosis_view;
}

View filterCdiffDiagnosis(View &diagnosis_view, int64_t cdiff_code, int tid) {
    std::vector<std::string> fieldNames = {"diag"};
    std::vector<View::ComparatorType> comparatorTypes = {View::EQUALS};
    std::vector<int64_t> constShares = {cdiff_code};

    View rcd_view = diagnosis_view;
    rcd_view.filterAndConditions(fieldNames, comparatorTypes, constShares, tid);
    return rcd_view;
}

View performSelfJoin(View &rcd_view, int tid) {
    // Use hash-based join with the bucket tag column
    std::string left_key = "pid";
    std::string right_key = "pid";
    auto rcd_view_copy = rcd_view; // Create a copy to avoid modifying the original view
    rcd_view._tableName = "r1";
    rcd_view_copy._tableName = "r2";
    auto joined_view = Views::hashJoin(rcd_view, rcd_view_copy, left_key, right_key);
    return joined_view;
}

View filterTimeAndRowConditions(View &joined_view, int tid) {
    if (joined_view.rowNum() == 0) return joined_view;

    // 定位列
    const int r1_time_plus_15_idx = joined_view.colIndex("r1.time_plus_15");
    const int r1_time_plus_56_idx = joined_view.colIndex("r1.time_plus_56");
    const int r2_time_idx = joined_view.colIndex("r2.time");
    const int r1_row_no_plus_1_idx = joined_view.colIndex("r1.row_no_plus_1");
    const int r2_row_no_idx = joined_view.colIndex("r2.row_no");
    if (r1_time_plus_15_idx < 0 || r1_time_plus_56_idx < 0 || r2_time_idx < 0 ||
        r1_row_no_plus_1_idx < 0 || r2_row_no_idx < 0) {
        return joined_view; // 列缺失就直接返回
    }

    auto &c_t15 = joined_view._dataCols[r1_time_plus_15_idx];
    auto &c_t56 = joined_view._dataCols[r1_time_plus_56_idx];
    auto &c_t2 = joined_view._dataCols[r2_time_idx];
    auto &c_r1p1 = joined_view._dataCols[r1_row_no_plus_1_idx];
    auto &c_r2 = joined_view._dataCols[r2_row_no_idx];

    const size_t n = c_t2.size();
    std::vector<int64_t> final_condition;
    final_condition.reserve(n);

    // 批量与并行设置
    const int batchSize = Conf::BATCH_SIZE;
    const int batchNum = static_cast<int>((n + batchSize - 1) / batchSize);

    // 计算每个 batch 的 tag 空间跨度
    const int strideLess64 = BoolLessBatchOperator::tagStride();
    const int strideAnd1 = BoolAndBatchOperator::tagStride();
    const int strideEq64 = BoolEqualBatchOperator::tagStride();
    const int totalStridePerBatch = strideLess64 + strideLess64 + strideAnd1 + strideEq64 + strideAnd1;

    if (Conf::BATCH_SIZE <= 0 || Conf::DISABLE_MULTI_THREAD) {
        // 单批执行（与原版逻辑一致）
        auto ge15 = BoolLessBatchOperator(&c_t15, &c_t2, 64, 0, tid,
                                          SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        auto le56 = BoolLessBatchOperator(&c_t2, &c_t56, 64, 0, tid,
                                          SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        auto inrng = BoolAndBatchOperator(&ge15, &le56, 1, 0, tid,
                                          SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        auto consec = BoolEqualBatchOperator(&c_r2, &c_r1p1, 64, 0, tid,
                                             SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        final_condition = BoolAndBatchOperator(&inrng, &consec, 1, 0, tid,
                                               SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    } else {
        // 多批并行：每个任务返回自己批次的 final_condition
        std::vector<std::future<std::vector<int64_t> > > futs(batchNum);
        for (int b = 0; b < batchNum; ++b) {
            futs[b] = ThreadPoolSupport::submit([&, b]() -> std::vector<int64_t> {
                const int start = b * batchSize;
                const int end = std::min(start + batchSize, static_cast<int>(n));
                if (start >= end) return {};

                // 切片
                std::vector<int64_t> t15(c_t15.begin() + start, c_t15.begin() + end);
                std::vector<int64_t> t56(c_t56.begin() + start, c_t56.begin() + end);
                std::vector<int64_t> t2(c_t2.begin() + start, c_t2.begin() + end);
                std::vector<int64_t> r1p1(c_r1p1.begin() + start, c_r1p1.begin() + end);
                std::vector<int64_t> r2(c_r2.begin() + start, c_r2.begin() + end);

                // 为该批分配独立 tag 段
                int bt = tid + b * totalStridePerBatch;

                auto ge15 = BoolLessBatchOperator(&t15, &t2, 64, 0, bt,
                                                  SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
                bt += strideLess64;
                auto le56 = BoolLessBatchOperator(&t2, &t56, 64, 0, bt,
                                                  SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
                bt += strideLess64;
                auto inrng = BoolAndBatchOperator(&ge15, &le56, 1, 0, bt,
                                                  SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
                bt += strideAnd1;
                auto consec = BoolEqualBatchOperator(&r2, &r1p1, 64, 0, bt,
                                                     SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
                bt += strideEq64;
                return BoolAndBatchOperator(&inrng, &consec, 1, 0, bt,
                                            SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
            });
        }
        // 拼接各批结果
        for (auto &f: futs) {
            auto part = f.get();
            final_condition.insert(final_condition.end(), part.begin(), part.end());
        }
    }

    // 应用过滤
    View filtered_view = joined_view;
    const int valid_col_idx = filtered_view.colNum() + View::VALID_COL_OFFSET;
    filtered_view._dataCols[valid_col_idx].assign(final_condition.begin(), final_condition.end());

    // 为安全起见选一个大于本函数内所有算子占用的 tag 作 clear 的 tid
    const int clearTid = tid + batchNum * totalStridePerBatch;
    filtered_view.clearInvalidEntries(clearTid);
    return filtered_view;
}

View selectDistinctPid(View &filtered_view, int tid) {
    if (filtered_view.rowNum() == 0) {
        std::vector<std::string> result_fields = {"pid"};
        std::vector<int> result_widths = {64};
        View result_view(result_fields, result_widths);
        return result_view;
    }

    std::vector<std::string> field_names = {"r1.pid"};
    filtered_view.select(field_names);
    // Apply distinct operation on all columns
    filtered_view.distinct(tid);
    return filtered_view;
}
