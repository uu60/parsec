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

// ---- Optimized Top-K path (replaces full sort + LIMIT) ----
View executeTopKByCount(View grouped_diag_cnt, int k, int tid);

// ---- Small helpers for Top-K ----
static View sliceRows(View &v, size_t start, size_t len);

static View concatSameSchema(View &a, View &b);

static View projectDiagCnt(View &v, std::string diag_field, std::string cnt_field);

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

    // ★ Bug fix: cohort shares must be created from cdiff_cohort_pid_data (NOT diagnosis_pid_data)
    auto cdiff_cohort_pid_shares = Secrets::boolShare(cdiff_cohort_pid_data, 2, 64, tid);

    View result_view;
    if (Comm::isServer()) {
        // Create tables
        auto diagnosis_view = createDiagnosisTable(diagnosis_pid_shares, diagnosis_diag_shares, diagnosis_extra_shares);
        auto cdiff_cohort_view = createCohortTable(cdiff_cohort_pid_shares);

        auto query_start = System::currentTimeMillis();

        // Execute query steps
        auto in_results = executeWhereInClause(diagnosis_view, cdiff_cohort_view);
        auto filtered_diagnosis = filterDiagnosisTable(diagnosis_view, in_results, tid);

        // GROUP BY diag, COUNT(*)  —— 内部会排序/分段；之后我们立即只保留 {diag, cnt}
        auto diag_cnt_view = executeGroupByCount(filtered_diagnosis, tid);

        // ★ Top-K：避免全排序；只拿 cnt 最大的前 10
        result_view = executeTopKByCount(std::move(diag_cnt_view), /*k=*/10, tid);

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
    // 逐行 insert（保守实现，与现有接口兼容）
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
    // 将有效位挂到 VALID 列并做隐蔽压缩（假定实现是数据无关访问）
    filtered_diagnosis._dataCols[filtered_diagnosis.colNum() + View::VALID_COL_OFFSET] = in_results;
    filtered_diagnosis.clearInvalidEntries(tid);
    return filtered_diagnosis;
}

View executeGroupByCount(View &filtered_diagnosis, int tid) {
    // GROUP BY diag
    std::string diag_field = "diag";
    auto group_heads = filtered_diagnosis.groupBy(diag_field, tid);

    size_t n = filtered_diagnosis.rowNum();
    if (n == 0) {
        std::vector<std::string> result_fields = {"diag", "cnt"};
        std::vector<int> result_widths = {64, 64};
        View result_view(result_fields, result_widths);
        return result_view;
    }

    // COUNT(*) as cnt
    std::vector<std::string> group_fields = {diag_field};
    filtered_diagnosis.count(group_fields, group_heads, "cnt", tid);


    // ★ 早投影：只保留 {diag, cnt}，大幅减少后续数据搬运
    return projectDiagCnt(filtered_diagnosis, "diag", "cnt");
}

// ---------------- Top-K 实现（迭代 2k 归并） ----------------

View executeTopKByCount(View grouped_diag_cnt, int k, int tid) {
    std::string count_field = "cnt";
    int cnt_idx = grouped_diag_cnt.colIndex(count_field);
    if (cnt_idx < 0) {
        // 兜底：没有 cnt 列就直接返回（不应发生）
        return grouped_diag_cnt;
    }

    size_t n = grouped_diag_cnt.rowNum();
    if (n <= (size_t) k) {
        // 短路：行数不超过 k，直接一次排序返回
        grouped_diag_cnt.sort(count_field, /*ascending=*/false, tid);
        return grouped_diag_cnt;
    }

    // 1) 取前 2k 行排序，保留前 k 作为缓冲
    size_t first_block = std::min(n, (size_t) (2 * k));
    View buf = sliceRows(grouped_diag_cnt, 0, first_block);
    buf.sort(count_field, /*ascending=*/false, tid);
    for (int c = 0; c < buf.colNum(); ++c) {
        buf._dataCols[c].resize(k);
    }

    // 2) 之后每次取接下来的 k 行，与缓冲拼成 2k，排序，再截断为 k
    size_t off = first_block;
    while (off < n) {
        size_t take = std::min((size_t) k, n - off);
        View chunk = sliceRows(grouped_diag_cnt, off, take);
        off += take;

        View merged = concatSameSchema(buf, chunk);
        merged.sort(count_field, /*ascending=*/false, tid);
        // 截断回 k
        for (int c = 0; c < merged.colNum(); ++c) {
            merged._dataCols[c].resize(k);
        }
        buf = std::move(merged);
    }

    return buf;
}

// ---- helpers ----

static View projectDiagCnt(View &v, std::string diag_field, std::string cnt_field) {
    int d = v.colIndex(diag_field);
    int c = v.colIndex(cnt_field);
    std::vector<std::string> fields = {diag_field, cnt_field};
    std::vector<int> widths = {64, 64};
    View out(fields, widths);
    out._dataCols.resize(2);
    if (d >= 0) out._dataCols[0] = v._dataCols[d];
    if (c >= 0) out._dataCols[1] = v._dataCols[c];
    return out;
}

static View sliceRows(View &v, size_t start, size_t len) {
    std::vector<std::string> fields = v._fieldNames;
    std::vector<int> widths = v._fieldWidths;
    View out(fields, widths);
    out._dataCols.resize(v.colNum());
    size_t end = start + len;
    for (int col = 0; col < v.colNum(); ++col) {
        if (start >= v._dataCols[col].size()) {
            out._dataCols[col].clear();
            continue;
        }
        end = std::min(end, v._dataCols[col].size());
        out._dataCols[col].assign(v._dataCols[col].begin() + start, v._dataCols[col].begin() + end);
    }
    return out;
}

static View concatSameSchema(View &a, View &b) {
    // 假设 a、b schema 相同
    View out(a._fieldNames, a._fieldWidths);
    out._dataCols.resize(a.colNum());
    for (int col = 0; col < a.colNum(); ++col) {
        out._dataCols[col] = a._dataCols[col];
        out._dataCols[col].insert(out._dataCols[col].end(), b._dataCols[col].begin(), b._dataCols[col].end());
    }
    return out;
}
