
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

#include "compute/batch/arith/ArithMultiplyBatchOperator.h"

static int64_t START_DATE = Math::randInt();
static int64_t END_DATE_EX = Math::randInt();
static int64_t DISCOUNT_MIN = Math::randInt();
static int64_t DISCOUNT_MAX = Math::randInt();
static int64_t QUANTITY_TH = Math::randInt();

void generateTestData(int lineitem_rows,
                      std::vector<int64_t> &l_shipdate_data,
                      std::vector<int64_t> &l_discount_data,
                      std::vector<int64_t> &l_quantity_data,
                      std::vector<int64_t> &l_extendedprice_data,
                      std::vector<int64_t> &l_revenue_data);

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

int main(int argc, char *argv[]) {
    System::init(argc, argv);
    DbConf::init();
    const int tid = (System::nextTask() << (32 - Conf::TASK_TAG_BITS));
    bool check_mode = Conf::_userParams.count("check") && Conf::_userParams["check"] == "true";

    int lineitem_rows = 1000;
    if (Conf::_userParams.count("rows")) {
        lineitem_rows = std::stoi(Conf::_userParams["rows"]);
    }
    if (Comm::isClient()) {
        Log::i("Validation: lineitem rows = {}", lineitem_rows);
    }

    std::vector<int64_t> l_shipdate_p, l_discount_p, l_quantity_p, l_extendedprice_p, l_revenue_p;
    generateTestData(lineitem_rows, l_shipdate_p, l_discount_p, l_quantity_p, l_extendedprice_p, l_revenue_p);
    if (check_mode && Comm::isClient()) {
        l_shipdate_p = {110, 150, 150, 90, 120};
        l_discount_p = {3, 2, 5, 3, 3};
        l_quantity_p = {5, 9, 9, 5, 11};
        l_extendedprice_p = {10, 7, 8, 6, 9};
        l_revenue_p.clear();
        for (size_t i = 0; i < l_extendedprice_p.size(); ++i) {
            l_revenue_p.push_back(l_extendedprice_p[i] * l_discount_p[i]);
        }
    }


    auto l_shipdate_b = Secrets::boolShare(l_shipdate_p, 2, 64, tid);
    auto l_discount_b = Secrets::boolShare(l_discount_p, 2, 64, tid);
    auto l_quantity_b = Secrets::boolShare(l_quantity_p, 2, 64, tid);
    auto l_extendedprice_b = Secrets::boolShare(l_extendedprice_p, 2, 64, tid);
    auto l_revenue_b = Secrets::boolShare(l_revenue_p, 2, 64, tid);

    int64_t start_b, end_b, dmin_b, dmax_b, q_b;
    int64_t start_plain = check_mode ? 100 : START_DATE;
    int64_t end_plain = check_mode ? 200 : END_DATE_EX;
    int64_t dmin_plain = check_mode ? 2 : DISCOUNT_MIN;
    int64_t dmax_plain = check_mode ? 4 : DISCOUNT_MAX;
    int64_t q_plain = check_mode ? 10 : QUANTITY_TH;
    if (Comm::isClient()) {
        auto splitXor = [&](int64_t plain, int dst0, int dst1, int tag) {
            int64_t s0 = Math::randInt();
            int64_t s1 = s0 ^ plain;
            Comm::send(s0, 64, dst0, tag);
            Comm::send(s1, 64, dst1, tag);
        };
        splitXor(start_plain, 0, 1, tid + 100);
        splitXor(end_plain, 0, 1, tid + 101);
        splitXor(dmin_plain, 0, 1, tid + 102);
        splitXor(dmax_plain, 0, 1, tid + 103);
        splitXor(q_plain, 0, 1, tid + 104);
    } else {
        Comm::receive(start_b, 64, 2, tid + 100);
        Comm::receive(end_b, 64, 2, tid + 101);
        Comm::receive(dmin_b, 64, 2, tid + 102);
        Comm::receive(dmax_b, 64, 2, tid + 103);
        Comm::receive(q_b, 64, 2, tid + 104);
    }

    int64_t sum_share = 0;
    if (Comm::isServer()) {
        auto v = createLineitemTable(l_shipdate_b, l_discount_b, l_quantity_b, l_extendedprice_b, l_revenue_b);

        auto t0 = System::currentTimeMillis();

        auto vf = filterAllConditions(v, start_b, end_b, dmin_b, dmax_b, q_b, tid + 200);

        sum_share = calculateRevenue(vf, tid + 300);

        auto t1 = System::currentTimeMillis();
        Log::i("Validation query (server {}) time: {}ms", Comm::rank(), (t1 - t0));
    }

    if (check_mode) {
        std::vector<int64_t> sum_vec;
        if (Comm::isServer()) {
            sum_vec = {sum_share};
        }
        auto plain = Secrets::arithReconstruct(sum_vec, 2, 64, tid + 400);
        if (Comm::isClient()) {
            int64_t revenue = plain.empty() ? 0 : plain[0];
            Log::i("CORRECTNESS_SCALAR {}", revenue);
        }
    }

    System::finalize();
    return 0;
}

void generateTestData(int lineitem_rows,
                      std::vector<int64_t> &l_shipdate_data,
                      std::vector<int64_t> &l_discount_data,
                      std::vector<int64_t> &l_quantity_data,
                      std::vector<int64_t> &l_extendedprice_data,
                      std::vector<int64_t> &l_revenue_data) {
    if (Comm::rank() == 2) {
        l_shipdate_data.reserve(lineitem_rows);
        l_discount_data.reserve(lineitem_rows);
        l_quantity_data.reserve(lineitem_rows);
        l_extendedprice_data.reserve(lineitem_rows);
        l_revenue_data.reserve(lineitem_rows);

        for (int i = 0; i < lineitem_rows; i++) {
            l_shipdate_data.push_back(Math::randInt());
            l_discount_data.push_back(Math::randInt());
            l_quantity_data.push_back(Math::randInt());
            l_extendedprice_data.push_back(Math::randInt());
            l_revenue_data.push_back(l_extendedprice_data[i] * l_discount_data[i]);
        }
    }
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
        View::GREATER_EQ,
        View::LESS,
        View::GREATER_EQ,
        View::LESS_EQ,
        View::LESS
    };
    std::vector<int64_t> consts = {
        start_date_b, end_date_b, discount_min_b, discount_max_b, quantity_th_b
    };

    v.filterAndConditions(cols, cmps, consts, false, tid);
    return v;
}

int64_t calculateRevenue(View &filtered_view, int tid) {
    if (filtered_view.rowNum() == 0) return 0;

    auto &rev_b = filtered_view._dataCols[filtered_view.colIndex("l_revenue")];
    auto &valid_b = filtered_view._dataCols[filtered_view.colNum() + View::VALID_COL_OFFSET];

    const int t0 = tid;
    const int t1 = tid + BoolToArithBatchOperator::tagStride();

    auto rev_a = BoolToArithBatchOperator(&rev_b, 64, 0, t0,
                                          SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    auto valid_a = BoolToArithBatchOperator(&valid_b, 64, 0, t1,
                                            SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

    const int t2 = t1 + ArithMultiplyBatchOperator::tagStride(64);
    auto masked = ArithMultiplyBatchOperator(&rev_a, &valid_a, 64, 0, t2,
                                             SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

    int64_t acc = 0;
    for (auto x: masked) acc += x;
    return acc;
}
