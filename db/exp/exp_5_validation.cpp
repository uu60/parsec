// Validation for:
// SELECT S.ID
// FROM (SELECT ID, MIN(CS) AS cs1, MAX(CS) AS cs2 FROM R WHERE year=2019 GROUP BY ID) S
// WHERE S.cs2 - S.cs1 > c
//
// 优化点：CS_PLUS 在明文生成阶段预计算并入表，查询阶段不再做安全加法/转换。
// 最终判断用 MIN(CS_PLUS) < MAX(CS)。

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
#include "parallel/ThreadPoolSupport.h"

#include <algorithm>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "compute/batch/bool/BoolAndBatchOperator.h"

// -------------------- 明文常量 --------------------
static constexpr int64_t PLAIN_YEAR = 2019;
static constexpr int64_t PLAIN_C = 10;

// -------------------- 声明 --------------------
void generateFixedData(std::vector<int64_t> &id_data,
                       std::vector<int64_t> &cs_data,
                       std::vector<int64_t> &year_data,
                       std::vector<int64_t> &cs_plus_data);

std::set<int64_t> computeExpectedIDs(std::vector<int64_t> &id,
                                     std::vector<int64_t> &cs,
                                     std::vector<int64_t> &year);

View createR(std::vector<int64_t> &id_b,
             std::vector<int64_t> &cs_b,
             std::vector<int64_t> &year_b,
             std::vector<int64_t> &cs_plus_b);

// 过滤 year=2019
View filterByYear(View v, int64_t year_share, int tid);

// 分组：同时 MIN(CS_PLUS) 与 MAX(CS)
View groupMinCsPlusMaxCs(View v, int tid);

// 根据 MIN(CS_PLUS) < MAX(CS) 过滤，并仅输出 ID
View finalizeByLess(View v, int tid);

// -------------------- 主流程 --------------------
int main(int argc, char *argv[]) {
    System::init(argc, argv);
    DbConf::init();
    auto tid = (System::nextTask() << (32 - Conf::TASK_TAG_BITS));

    // 1) 固定明文数据（含 CS_PLUS 预计算）
    std::vector<int64_t> id_plain, cs_plain, year_plain, cs_plus_plain;
    generateFixedData(id_plain, cs_plain, year_plain, cs_plus_plain);

    // 2) 分享数据
    auto id_b = Secrets::boolShare(id_plain, 2, 64, tid);
    auto cs_b = Secrets::boolShare(cs_plain, 2, 64, tid);
    auto year_b = Secrets::boolShare(year_plain, 2, 64, tid);
    auto cs_plus_b = Secrets::boolShare(cs_plus_plain, 2, 64, tid);

    // 3) 分享常量：year 用 XOR 布尔分享（y0 ^ y1 = 2019）
    int64_t year_share = 0;
    if (Comm::isClient()) {
        int64_t y0 = Math::randInt();
        int64_t y1 = y0 ^ PLAIN_YEAR;
        Comm::send(y0, 64, 0, tid);
        Comm::send(y1, 64, 1, tid);
    } else {
        Comm::receive(year_share, 64, 2, tid);
    }

    // 期望结果（仅 client 侧）
    std::set<int64_t> expected;
    if (Comm::rank() == 2) {
        expected = computeExpectedIDs(id_plain, cs_plain, year_plain);
    }

    // 4) 服务器端执行查询物理计划
    if (Comm::isServer()) {
        auto t0 = System::currentTimeMillis();

        // 建表：直接包含 CS_PLUS
        auto v = createR(id_b, cs_b, year_b, cs_plus_b);
        auto vf = filterByYear(v, year_share, tid + 100);
        auto va = groupMinCsPlusMaxCs(vf, tid + 200);
        auto out = finalizeByLess(va, tid + 300);

        auto t1 = System::currentTimeMillis();
        Log::i("Validation query time: {}ms", (t1 - t0));

        Views::revealAndPrint(out);
    }

    std::vector<int> ev;
    ev.assign(expected.begin(), expected.end());
    if (Comm::isClient()) {
        Log::i("expected: {}", StringUtils::vecToString(ev));
    }

    System::finalize();
    return 0;
}

// -------------------- 实现 --------------------

// 固定数据：覆盖多种情况；这里在明文就计算 CS_PLUS = CS + c
void generateFixedData(std::vector<int64_t> &id,
                       std::vector<int64_t> &cs,
                       std::vector<int64_t> &year,
                       std::vector<int64_t> &cs_plus) {
    if (Comm::rank() != 2) return;

    struct Row {
        int64_t id, cs, y;
    };
    std::vector<Row> rows = {
        // id=1: 2019 两条 (50,80) -> range=30>10 (true)
        {1, 50, 2019}, {1, 80, 2019}, {1, 10, 2018},
        // id=2: 2019 单条 -> range=0<=10 (false)
        {2, 100, 2019}, {2, 90, 2018},
        // id=3: 2019 两条 (70,85) -> range=15>10 (true)
        {3, 70, 2019}, {3, 85, 2019},
        // id=4: 无 2019
        {4, 200, 2020},
        // id=5: 2019 两条 (30,31) -> range=1<=10 (false)
        {5, 30, 2019}, {5, 31, 2019},
        // id=6: 无 2019
        {6, 5, 2018},
        // id=7: 2019 单条 (60) -> range=0<=10 (false)
        {7, 60, 2019}
    };

    id.reserve(rows.size());
    cs.reserve(rows.size());
    year.reserve(rows.size());
    cs_plus.reserve(rows.size());

    for (auto &r: rows) {
        id.push_back(r.id);
        cs.push_back(r.cs);
        year.push_back(r.y);
        // 明文预计算
        uint64_t sum = static_cast<uint64_t>(r.cs) + static_cast<uint64_t>(PLAIN_C);
        cs_plus.push_back(static_cast<int64_t>(sum)); // 2^64 环
    }
}

// 期望结果（明文 SQL 语义）
std::set<int64_t> computeExpectedIDs(std::vector<int64_t> &id,
                                     std::vector<int64_t> &cs,
                                     std::vector<int64_t> &year) {
    std::map<int64_t, std::vector<int64_t> > m;
    for (size_t i = 0; i < id.size(); ++i) {
        if (year[i] == PLAIN_YEAR) m[id[i]].push_back(cs[i]);
    }
    std::set<int64_t> ans;
    for (auto &kv: m) {
        if (kv.second.empty()) continue;
        auto mn = *std::min_element(kv.second.begin(), kv.second.end());
        auto mx = *std::max_element(kv.second.begin(), kv.second.end());
        if (mx - mn > PLAIN_C) ans.insert(kv.first);
    }
    return ans; // 预期：{1,3}
}

View createR(std::vector<int64_t> &id_b,
             std::vector<int64_t> &cs_b,
             std::vector<int64_t> &year_b,
             std::vector<int64_t> &cs_plus_b) {
    std::string tname = "R";
    std::vector<std::string> fields = {"ID", "CS", "year", "CS_PLUS"};
    std::vector<int> widths = {64, 64, 64, 64};
    Table t(tname, fields, widths, "");
    size_t n = id_b.size();
    for (size_t i = 0; i < n; ++i) {
        t.insert({id_b[i], cs_b[i], year_b[i], cs_plus_b[i]});
    }
    return Views::selectAll(t);
}

View filterByYear(View v, int64_t year_share, int tid) {
    std::vector<std::string> fields = {"year"};
    std::vector<View::ComparatorType> ops = {View::EQUALS};
    std::vector<int64_t> consts = {year_share};
    v.filterAndConditions(fields, ops, consts, tid);
    return v;
}

View groupMinCsPlusMaxCs(View v, int tid) {
    if (v.rowNum() == 0) return v;
    auto heads = v.groupBy("ID", tid);
    // 新接口：同时计算 left=min(CS_PLUS), right=max(CS)
    v.minAndMax(heads, "CS_PLUS", "CS", "cs1_plus", "cs2", tid + 10);
    return v;
}

View finalizeByLess(View v, int tid) {
    if (v.rowNum() == 0) return v;
    int a = v.colIndex("cs1_plus");
    int b = v.colIndex("cs2");
    if (a < 0 || b < 0) {
        Log::e("Aggregated columns missing");
        return v;
    }

    auto &min_plus = v._dataCols[a];
    auto &max_cs = v._dataCols[b];

    auto valid = BoolLessBatchOperator(&min_plus, &max_cs, 64, 0, tid,
                                       SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

    valid = BoolAndBatchOperator(&valid, &v._dataCols[v.colNum() + View::VALID_COL_OFFSET], 1, 0,
                                 tid + BoolLessBatchOperator::tagStride(),
                                 SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

    v._dataCols[v.colNum() + View::VALID_COL_OFFSET] = std::move(valid);

    std::vector<std::string> fields = {"ID"};
    std::vector<int> widths = {64};
    View out(fields, widths);
    if (v.rowNum() > 0) {
        out._dataCols[0] = v._dataCols[v.colIndex("ID")];
        out._dataCols[out.colNum() + View::VALID_COL_OFFSET]   =
            v._dataCols[v.colNum() + View::VALID_COL_OFFSET];
        out._dataCols[out.colNum() + View::PADDING_COL_OFFSET] =
            std::vector<int64_t>(out._dataCols[0].size(), 0);
    }
    return out;
}
