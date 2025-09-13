// tpch_q6_validation.cpp
// Validation for TPCH-Q6-like query:
// SELECT sum(l_extendedprice*l_discount) AS revenue
// FROM lineitem
// WHERE l_shipdate in [DATE, DATE+1y)
//   AND l_discount BETWEEN (d-1) AND (d+1)
//   AND l_quantity < Q
//
// 关键点：明文阶段预计算 l_revenue 并入表；一次性 AND 组合 5 个谓词；
// 客户端计算期望值，服务端求和并将算术份额发回客户端做重构。

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

#include "compute/batch/bool/BoolToArithBatchOperator.h"

#include <string>
#include <vector>
#include <algorithm>
#include <cstdint>

#include "../include/conf/DbConf.h"
#include "compute/batch/arith/ArithMultiplyBatchOperator.h"
#include "parallel/ThreadPoolSupport.h"

// -------------------- 固定常量 --------------------
static constexpr int64_t START_DATE = 19940101; // [1994-01-01,
static constexpr int64_t END_DATE_EX = 19950101; //  1995-01-01)
static constexpr int64_t DISCOUNT_MIN = 5; // 中心 6 ± 1
static constexpr int64_t DISCOUNT_MAX = 7;
static constexpr int64_t QUANTITY_TH = 24;

// -------------------- 声明 --------------------
void generateFixedData(
    int lineitem_rows,
    std::vector<int64_t> &l_shipdate,
    std::vector<int64_t> &l_discount,
    std::vector<int64_t> &l_quantity,
    std::vector<int64_t> &l_extendedprice,
    std::vector<int64_t> &l_revenue);

int64_t computeExpectedRevenue(
    const std::vector<int64_t> &l_shipdate,
    const std::vector<int64_t> &l_discount,
    const std::vector<int64_t> &l_quantity,
    const std::vector<int64_t> &l_revenue);

View createLineitemTable(
    std::vector<int64_t> &l_shipdate_b,
    std::vector<int64_t> &l_discount_b,
    std::vector<int64_t> &l_quantity_b,
    std::vector<int64_t> &l_extendedprice_b,
    std::vector<int64_t> &l_revenue_b);

View filterAllConditions(
    View v,
    int64_t start_date_b,
    int64_t end_date_b,
    int64_t discount_min_b,
    int64_t discount_max_b,
    int64_t quantity_th_b,
    int tid);

int64_t calculateRevenue(View &filtered_view, int tid);

// -------------------- 主流程 --------------------
int main(int argc, char *argv[]) {
    System::init(argc, argv);
    DbConf::init();
    const int tid = (System::nextTask() << (32 - Conf::TASK_TAG_BITS));

    // 行数参数（可选，默认 10）
    int lineitem_rows = 10;
    if (Conf::_userParams.count("rows")) {
        lineitem_rows = std::stoi(Conf::_userParams["rows"]);
    }
    if (Comm::isClient()) {
        Log::i("Validation: lineitem rows = {}", lineitem_rows);
    }

    // 1) 客户端生成固定明文数据，并在明文阶段预计算 l_revenue
    std::vector<int64_t> l_shipdate_p, l_discount_p, l_quantity_p, l_extendedprice_p, l_revenue_p;
    generateFixedData(lineitem_rows, l_shipdate_p, l_discount_p, l_quantity_p, l_extendedprice_p, l_revenue_p);

    // 2) 客户端计算期望值
    int64_t expected_revenue = 0;
    if (Comm::rank() == 2) {
        expected_revenue = computeExpectedRevenue(l_shipdate_p, l_discount_p, l_quantity_p, l_revenue_p);
        Log::i("[expected] revenue = {}", expected_revenue);
    }

    // 3) 分享数据（布尔分享）
    auto l_shipdate_b = Secrets::boolShare(l_shipdate_p, 2, 64, tid);
    auto l_discount_b = Secrets::boolShare(l_discount_p, 2, 64, tid);
    auto l_quantity_b = Secrets::boolShare(l_quantity_p, 2, 64, tid);
    auto l_extendedprice_b = Secrets::boolShare(l_extendedprice_p, 2, 64, tid);
    auto l_revenue_b = Secrets::boolShare(l_revenue_p, 2, 64, tid);

    // 4) 客户端将常量按 XOR 语义分享给两台服务端
    int64_t start_b, end_b, dmin_b, dmax_b, q_b;
    if (Comm::isClient()) {
        auto splitXor = [&](int64_t plain, int dst0, int dst1, int tag) {
            int64_t s0 = Math::randInt();
            int64_t s1 = s0 ^ plain;
            Comm::send(s0, 64, dst0, tag);
            Comm::send(s1, 64, dst1, tag);
        };
        splitXor(START_DATE, 0, 1, tid + 100);
        splitXor(END_DATE_EX, 0, 1, tid + 101);
        splitXor(DISCOUNT_MIN, 0, 1, tid + 102);
        splitXor(DISCOUNT_MAX, 0, 1, tid + 103);
        splitXor(QUANTITY_TH, 0, 1, tid + 104);
    } else {
        Comm::receive(start_b, 64, 2, tid + 100);
        Comm::receive(end_b, 64, 2, tid + 101);
        Comm::receive(dmin_b, 64, 2, tid + 102);
        Comm::receive(dmax_b, 64, 2, tid + 103);
        Comm::receive(q_b, 64, 2, tid + 104);
    }

    // 5) 服务端查询执行 & 把算术份额发送回客户端重构
    if (Comm::isServer()) {
        auto v = createLineitemTable(l_shipdate_b, l_discount_b, l_quantity_b, l_extendedprice_b, l_revenue_b);

        auto t0 = System::currentTimeMillis();

        // 一次性过滤五个谓词，并 clearInvalidEntries 避免无效行参与求和
        auto vf = filterAllConditions(v, start_b, end_b, dmin_b, dmax_b, q_b, tid + 200);

        // 将 l_revenue（布尔分享）转算术，再局部求和，得到本端**算术份额**的总和
        int64_t sum_share = calculateRevenue(vf, tid + 300);

        auto t1 = System::currentTimeMillis();
        Log::i("Validation query (server {}) time: {}ms", Comm::rank(), (t1 - t0));

        // 把算术份额发给客户端，由客户端重构明文
        Comm::send(sum_share, 64, 2, tid + 400 + Comm::rank()); // tag 区分两台 server
    }

    // 6) 客户端重构并对比
    if (Comm::isClient()) {
        int64_t s0 = 0, s1 = 0;
        Comm::receive(s0, 64, 0, tid + 400 + 0);
        Comm::receive(s1, 64, 1, tid + 400 + 1);
        // 算术份额重构（mod 2^64）
        unsigned __int128 recon = (unsigned long long) s0 + (unsigned long long) s1;
        int64_t actual_revenue = (uint64_t) recon; // 环上还原

        Log::i("[actual  ] revenue = {}", actual_revenue);
        Log::i("Validation {}", (actual_revenue == expected_revenue ? "PASS ✅" : "FAIL ❌"));
    }

    System::finalize();
    return 0;
}

// -------------------- 实现 --------------------

// 固定 10 行模板；若 rows > 10，会按模板循环填充。
void generateFixedData(
    int lineitem_rows,
    std::vector<int64_t> &l_shipdate,
    std::vector<int64_t> &l_discount,
    std::vector<int64_t> &l_quantity,
    std::vector<int64_t> &l_extendedprice,
    std::vector<int64_t> &l_revenue) {
    if (Comm::rank() != 2) return;

    std::vector<int64_t> ship = {
        19940201, 19940301, 19940401, 19940501, 19940601, // 0..4 in [1994,1995)
        19930101, 19950201, 19960101, 19970101, 19980101 // 5..9 out
    };
    std::vector<int64_t> disc = {
        5, 6, 7, 5, 6, // 0..4 in [5,7]
        3, 4, 8, 9, 10 // 5..9 out of [5,7]
    };
    std::vector<int64_t> qty = {
        10, 15, 20, 23, 22, // 0..4 < 24
        25, 30, 35, 40, 45 // 5..9 >= 24
    };
    std::vector<int64_t> price = {
        100000, 200000, 300000, 400000, 500000, // 1000.00, 2000.00, ...
        600000, 700000, 800000, 900000, 1000000
    };

    l_shipdate.reserve(lineitem_rows);
    l_discount.reserve(lineitem_rows);
    l_quantity.reserve(lineitem_rows);
    l_extendedprice.reserve(lineitem_rows);
    l_revenue.reserve(lineitem_rows);

    for (int i = 0; i < lineitem_rows; ++i) {
        int idx = i % 10;
        l_shipdate.push_back(ship[idx]);
        l_discount.push_back(disc[idx]);
        l_quantity.push_back(qty[idx]);
        l_extendedprice.push_back(price[idx]);
        // 明文预计算：revenue = extendedprice * discount （整型按 2^64 环）
        l_revenue.push_back(price[idx] * disc[idx]);
    }
}

// 期望结果（明文）：筛选命中行的 l_revenue 之和
int64_t computeExpectedRevenue(
    const std::vector<int64_t> &l_shipdate,
    const std::vector<int64_t> &l_discount,
    const std::vector<int64_t> &l_quantity,
    const std::vector<int64_t> &l_revenue) {
    unsigned __int128 acc = 0;
    const size_t n = l_shipdate.size();
    for (size_t i = 0; i < n; ++i) {
        bool ok =
                (l_shipdate[i] >= START_DATE) &&
                (l_shipdate[i] < END_DATE_EX) &&
                (l_discount[i] >= DISCOUNT_MIN) &&
                (l_discount[i] <= DISCOUNT_MAX) &&
                (l_quantity[i] < QUANTITY_TH);
        if (ok) acc += (unsigned long long) l_revenue[i];
    }
    return (uint64_t) acc; // mod 2^64
}

View createLineitemTable(
    std::vector<int64_t> &l_shipdate_b,
    std::vector<int64_t> &l_discount_b,
    std::vector<int64_t> &l_quantity_b,
    std::vector<int64_t> &l_extendedprice_b,
    std::vector<int64_t> &l_revenue_b) {
    std::string tname = "lineitem";
    std::vector<std::string> fields = {
        "l_shipdate", "l_discount", "l_quantity", "l_extendedprice", "l_revenue"
    };
    std::vector<int> widths = {64, 64, 64, 64, 64};
    Table t(tname, fields, widths, "");

    const size_t n = l_shipdate_b.size();
    for (size_t i = 0; i < n; ++i) {
        t.insert({
            l_shipdate_b[i], l_discount_b[i], l_quantity_b[i],
            l_extendedprice_b[i], l_revenue_b[i]
        });
    }
    return Views::selectAll(t);
}

// 一次性应用 5 个谓词（AND），并 clearInvalidEntries
View filterAllConditions(
    View v,
    int64_t start_date_b,
    int64_t end_date_b,
    int64_t discount_min_b,
    int64_t discount_max_b,
    int64_t quantity_th_b,
    int tid) {
    std::vector<std::string> cols = {
        "l_shipdate", "l_shipdate",
        "l_discount", "l_discount",
        "l_quantity"
    };
    std::vector<View::ComparatorType> cmps = {
        View::GREATER_EQ, // l_shipdate >= start
        View::LESS, // l_shipdate <  end
        View::GREATER_EQ, // l_discount >= dmin
        View::LESS_EQ, // l_discount <= dmax
        View::LESS // l_quantity <  Q
    };
    std::vector<int64_t> consts = {
        start_date_b, end_date_b, discount_min_b, discount_max_b, quantity_th_b
    };

    v.filterAndConditions(cols, cmps, consts, tid);
    return v;
}

// 将 l_revenue（布尔分享）→ 算术分享，然后局部求和，得到**本端算术份额**总和
int64_t calculateRevenue(View &filtered_view, int tid) {
    if (filtered_view.rowNum() == 0) return 0;

    // 1) 取列
    auto &rev_b = filtered_view._dataCols[filtered_view.colIndex("l_revenue")]; // 布尔份额
    auto &valid_b = filtered_view._dataCols[filtered_view.colNum() + View::VALID_COL_OFFSET]; // 布尔份额(0/1)

    // 2) 转算术份额
    const int t0 = tid;
    const int t1 = tid + BoolToArithBatchOperator::tagStride();

    std::vector<int64_t> rev_a, valid_a; // 算术份额
    if (DbConf::BASELINE_MODE) {
        rev_a = BoolToArithBatchOperator(&rev_b, 64, 0, t0,
                                         SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        valid_a = BoolToArithBatchOperator(&valid_b, 64, 0, t1,
                                           SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    } else {
        auto f = ThreadPoolSupport::submit([&] {
            return BoolToArithBatchOperator(&rev_b, 64, 0, t0,
                                            SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        });
        valid_a = BoolToArithBatchOperator(&valid_b, 64, 0, t1,
                                           SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        rev_a = f.get();
    }

    // 3) 逐元素乘法做掩码：masked_i = revenue_i * valid_i
    const int t2 = t1 + ArithMultiplyBatchOperator::tagStride(64);
    auto masked = ArithMultiplyBatchOperator(&rev_a, &valid_a, 64, 0, t2,
                                             SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

    // 4) 本端局部求和（这是全局和的“算术份额”）
    unsigned __int128 acc = 0;
    for (auto x: masked) acc += (unsigned long long) x;
    return (uint64_t) acc; // 2^64 环
}
