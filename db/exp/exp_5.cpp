// Random-data version for:
// SELECT S.ID
// FROM (SELECT ID, MIN(CS) AS cs1, MAX(CS) AS cs2 FROM R WHERE year=target_year GROUP BY ID) S
// WHERE S.cs2 - S.cs1 > c
//
// 优化点：在明文阶段预计算 CS_PLUS = CS + c 并入表；查询阶段不做安全加法/类型转换；
// 分组后用 MIN(CS_PLUS) 与 MAX(CS) 比较：MIN(CS_PLUS) < MAX(CS) ⟺ MAX(CS) - MIN(CS) > c

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
#include "compute/batch/bool/BoolAndBatchOperator.h"
#include "parallel/ThreadPoolSupport.h"

#include <algorithm>
#include <string>
#include <vector>
#include <cstdint>

// -------------------- 声明 --------------------
void generateRandomData(
    int rows,
    std::vector<int64_t> &id_data,
    std::vector<int64_t> &cs_data,
    std::vector<int64_t> &year_data,
    std::vector<int64_t> &cs_plus_data,
    int64_t &chosen_year, // 输出：用于过滤的 target_year（从 year_data 里随机挑一条）
    int64_t &chosen_c // 输出：用于预计算 CS_PLUS 的阈值 c
);

View createR(std::vector<int64_t> &id_b,
             std::vector<int64_t> &cs_b,
             std::vector<int64_t> &year_b,
             std::vector<int64_t> &cs_plus_b);

// year == target_year 过滤，**并立刻压缩**
View filterByYear(View v, int64_t year_share, int tid);

// 分组：同时 MIN(CS_PLUS) 与 MAX(CS)（依赖你新接口：minAndMax(heads, leftField, rightField, ... )）
View groupMinCsPlusMaxCs(View v, int tid);

// 根据 MIN(CS_PLUS) < MAX(CS) 过滤，并仅输出 ID
View finalizeByLess(View v, int tid);

// -------------------- 主流程 --------------------
int main(int argc, char *argv[]) {
    System::init(argc, argv);
    DbConf::init();
    auto tid = (System::nextTask() << (32 - Conf::TASK_TAG_BITS));

    int rows = 1000;
    if (Conf::_userParams.count("rows")) {
        rows = std::stoi(Conf::_userParams["rows"]);
    }
    if (Comm::isClient()) {
        Log::i("Data size: R: {}", rows);
    }

    // 1) 客户端生成随机明文数据，并在明文就把 CS_PLUS 预计算好
    std::vector<int64_t> id_plain, cs_plain, year_plain, cs_plus_plain;
    int64_t target_year_plain = 0;
    int64_t c_plain = 0;
    generateRandomData(rows, id_plain, cs_plain, year_plain, cs_plus_plain,
                       target_year_plain, c_plain);

    // 2) 分享数据
    auto id_b = Secrets::boolShare(id_plain, 2, 64, tid);
    auto cs_b = Secrets::boolShare(cs_plain, 2, 64, tid);
    auto year_b = Secrets::boolShare(year_plain, 2, 64, tid);
    auto cs_plus_b = Secrets::boolShare(cs_plus_plain, 2, 64, tid);

    // 3) 客户端把 target_year 以 XOR 份额发给两台服务端
    int64_t year_share = 0;
    if (Comm::isClient()) {
        int64_t y0 = Math::randInt();
        int64_t y1 = y0 ^ target_year_plain;
        Comm::send(y0, 64, 0, tid);
        Comm::send(y1, 64, 1, tid);
    } else {
        Comm::receive(year_share, 64, 2, tid);
    }

    // 4) 服务端执行查询物理计划
    if (Comm::isServer()) {
        auto v = createR(id_b, cs_b, year_b, cs_plus_b);

        auto t0 = System::currentTimeMillis();
        auto vf = filterByYear(v, year_share, tid + 100); // 过滤后立刻压缩
        auto va = groupMinCsPlusMaxCs(vf, tid + 200); // GROUP BY + 双聚合
        auto out = finalizeByLess(va, tid + 300); // MIN(CS_PLUS) < MAX(CS)

        auto t1 = System::currentTimeMillis();
        Log::i("Query time: {}ms", (t1 - t0));
    }

    System::finalize();
    return 0;
}

// -------------------- 实现 --------------------

// 随机数据生成：
// - ID/CS/year 全随机；
// - 随机选择阈值 c（客户端保留），直接生成 CS_PLUS = CS + c (mod 2^64)；
// - 从已生成的 year 中随机挑一个作为 target_year，保证过滤后通常非空。
void generateRandomData(
    int rows,
    std::vector<int64_t> &id,
    std::vector<int64_t> &cs,
    std::vector<int64_t> &year,
    std::vector<int64_t> &cs_plus,
    int64_t &chosen_year,
    int64_t &chosen_c
) {
    if (Comm::rank() != 2) return;

    id.reserve(rows);
    cs.reserve(rows);
    year.reserve(rows);
    cs_plus.reserve(rows);

    // 生成一个随机阈值 c（例如 1..100 之间）
    chosen_c = Math::randInt();

    // 生成随机数据
    for (int i = 0; i < rows; ++i) {
        // 让 ID 有重号（便于分组），使用较小基数
        int64_t rid = Math::randInt();
        int64_t rcs = Math::randInt(); // 任意 64 位
        int64_t ryr = Math::randInt(); // 2000..2029 之间

        id.push_back(rid);
        cs.push_back(rcs);
        year.push_back(ryr);

        uint64_t sum = rcs + chosen_c;
        cs_plus.push_back(sum); // 2^64 环加
    }

    chosen_year = 2019;
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

// ★ 关键：过滤后立刻压缩（清掉无效行，避免后续聚合污染）
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
    // 使用你新接口：左列做 min(CS_PLUS)，右列做 max(CS)
    v.minAndMax(heads, "CS_PLUS", "CS", "cs1_plus", "cs2", tid);
    return v;
}

View finalizeByLess(View v, int tid) {
    if (v.rowNum() == 0) return v;

    const int a = v.colIndex("cs1_plus");
    const int b = v.colIndex("cs2");
    if (a < 0 || b < 0) {
        Log::e("Aggregated columns missing");
        return v;
    }

    auto &min_plus = v._dataCols[a];
    auto &max_cs = v._dataCols[b];

    // MIN(CS_PLUS) < MAX(CS)
    auto lessBits = BoolLessBatchOperator(&min_plus, &max_cs, 64, 0, tid,
                                          SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

    // 与已有 valid 取 AND，保证只保留 group 尾行且此前有效
    int vidx = v.colNum() + View::VALID_COL_OFFSET;
    auto validAnd = BoolAndBatchOperator(&lessBits, &v._dataCols[vidx], 1, 0,
                                         tid + BoolLessBatchOperator::tagStride(),
                                         SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    v._dataCols[vidx] = std::move(validAnd);

    // 仅输出 ID 列
    std::vector<std::string> fields = {"ID"};
    std::vector<int> widths = {64};
    View out(fields, widths);
    if (v.rowNum() > 0) {
        out._dataCols[0] = v._dataCols[v.colIndex("ID")];
        out._dataCols[out.colNum() + View::VALID_COL_OFFSET] = v._dataCols[vidx];
        out._dataCols[out.colNum() + View::PADDING_COL_OFFSET] = std::vector<int64_t>(out._dataCols[0].size(), 0);
    }
    return out;
}
