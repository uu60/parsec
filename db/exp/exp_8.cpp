// Optimized version for TPCH Q13 (custdist)
// Based on the optimized SQL query structure:
// WITH cnt AS (
//   SELECT o_custkey, COUNT(*) AS c_count
//   FROM orders
//   WHERE o_comment = '[WORD]'
//   GROUP BY o_custkey
// ),
// hist AS (              -- 只包含 c_count >= 1 的桶
//   SELECT c_count, COUNT(*) AS custdist
//   FROM cnt
//   GROUP BY c_count
// ),
// zeros AS (             -- 计算 c_count = 0 的桶
//   SELECT 0 AS c_count,
//          (SELECT COUNT(*) FROM customer)        -- 全部顾客
//        - (SELECT COUNT(*) FROM cnt) AS custdist -- 至少有一条匹配订单的顾客
// )
// SELECT c_count, custdist
// FROM hist
// UNION ALL
// SELECT c_count, custdist FROM zeros
// ORDER BY custdist DESC, c_count DESC;

#include "secret/Secrets.h"
#include "utils/System.h"
#include "conf/DbConf.h"

#include "../include/basis/Table.h"
#include "../include/basis/View.h"
#include "../include/basis/Views.h"
#include "../include/operator/SelectSupport.h"

#include "parallel/ThreadPoolSupport.h"
#include "utils/Log.h"
#include "utils/Math.h"
#include "utils/StringUtils.h"

#include <vector>
#include <map>
#include <algorithm>

#include "compute/batch/arith/ArithToBoolBatchOperator.h"
#include "compute/batch/bool/BoolAndBatchOperator.h"
#include "compute/batch/bool/BoolToArithBatchOperator.h"
#include "compute/batch/bool/BoolXorBatchOperator.h"

// ---------- 使用与 exp_8_validation_o 相同的固定数据生成 ----------
// base 有 7 个客户，12 条订单，过滤后得到各客户匹配订单数：
// c1: 2  (两条 flag=0)
// c2: 1  (一条 flag=0)
// c3: 0  (两条 flag=1)
// c4: 0  (无订单)
// c5: 2  (三条中两条 flag=0)
// c6: 3  (三条 flag=0)
// c7: 0  (一条 flag=1)
// => 直方图：{ (0,3), (1,1), (2,2), (3,1) }

struct OrderRow {
    int64_t cust;
    int64_t orderkey;
    int64_t flag;
};

void generateTestData(int customer_rows, int orders_rows,
                      std::vector<int64_t> &c_custkey_data,
                      std::vector<int64_t> &o_custkey_data,
                      std::vector<int64_t> &o_orderkey_data,
                      std::vector<int64_t> &o_comment_flag_data) {
    if (Comm::rank() == 2) {
        // Generate random customer data
        c_custkey_data.reserve(customer_rows);

        for (int i = 0; i < customer_rows; i++) {
            int64_t custkey = Math::randInt();
            c_custkey_data.push_back(custkey);
        }

        // Generate random orders data
        o_custkey_data.reserve(orders_rows);
        o_orderkey_data.reserve(orders_rows);
        o_comment_flag_data.reserve(orders_rows);

        for (int i = 0; i < orders_rows; i++) {
            int64_t custkey = Math::randInt();
            int64_t orderkey = Math::randInt();
            int64_t comment_flag = Math::randInt(); // 0 or 1

            o_custkey_data.push_back(custkey);
            o_orderkey_data.push_back(orderkey);
            o_comment_flag_data.push_back(comment_flag);
        }
    }
}

// ---------- 表创建函数 ----------
View createCustomerTable(std::vector<int64_t> &c_custkey_shares) {
    std::string table_name = "customer";
    std::vector<std::string> fields = {"c_custkey"};
    std::vector<int> widths = {64};
    Table t(table_name, fields, widths, "");
    for (size_t i = 0; i < c_custkey_shares.size(); ++i) {
        t.insert({c_custkey_shares[i]});
    }
    return Views::selectAll(t);
}

View createOrdersTable(std::vector<int64_t> &o_custkey_shares,
                       std::vector<int64_t> &o_orderkey_shares,
                       std::vector<int64_t> &o_comment_flag_shares) {
    std::string table_name = "orders";
    std::vector<std::string> fields = {"o_custkey", "o_orderkey", "o_comment_flag"};
    std::vector<int> widths = {64, 64, 64};
    Table t(table_name, fields, widths, "");
    for (size_t i = 0; i < o_custkey_shares.size(); ++i) {
        t.insert({o_custkey_shares[i], o_orderkey_shares[i], o_comment_flag_shares[i]});
    }
    return Views::selectAll(t);
}

// ---------- CTE cnt: 过滤并按客户分组计数 ----------
View createCntCTE(View &orders_view, int64_t word, int tid) {
    // Filter orders where o_comment_flag = word (0)
    std::vector<std::string> fields = {"o_comment_flag"};
    std::vector<View::ComparatorType> cmps = {View::EQUALS};
    std::vector<int64_t> consts = {word};
    View filtered_orders = orders_view;
    filtered_orders.filterAndConditions(fields, cmps, consts, false, tid);

    // Group by o_custkey and count
    std::vector<std::string> group_fields = {"o_custkey"};
    filtered_orders.select(group_fields);

    auto gb = filtered_orders.groupBy("o_custkey", tid + 100);
    filtered_orders.count(group_fields, gb, "c_count", false, tid + 200);

    // Rename fields for clarity
    if (!filtered_orders._fieldNames.empty() && filtered_orders._fieldNames.size() >= 2) {
        filtered_orders._fieldNames[0] = "o_custkey";
        filtered_orders._fieldNames[1] = "c_count";
    }

    return filtered_orders;
}

// ---------- CTE hist: 对计数值分组统计 (只包含 c_count >= 1 的桶) ----------
View createHistCTE(View &cnt_view, int tid) {
    // Group by c_count and count customers
    std::vector<std::string> group_fields = {"c_count"};
    cnt_view.select(group_fields);

    auto gb = cnt_view.groupBy("c_count", tid + 300);
    cnt_view.count(group_fields, gb, "custdist", false, tid + 400);

    // Rename fields for clarity
    if (!cnt_view._fieldNames.empty() && cnt_view._fieldNames.size() >= 2) {
        cnt_view._fieldNames[0] = "c_count";
        cnt_view._fieldNames[1] = "custdist";
    }

    return cnt_view;
}

// ---------- CTE zeros: 使用 in 操作计算 c_count = 0 的桶 ----------
View createZerosCTE(View &customer_view, View &cnt_view, int tid) {
    // 取列与 valid
    const int cust_key_idx = customer_view.colIndex("c_custkey");
    const int cnt_key_idx = cnt_view.colIndex("o_custkey");
    const int cust_valid_i = customer_view.colNum() + View::VALID_COL_OFFSET;
    const int cnt_valid_i = cnt_view.colNum() + View::VALID_COL_OFFSET;

    auto &customer_keys = customer_view._dataCols[cust_key_idx];
    auto &cnt_keys = cnt_view._dataCols[cnt_key_idx];
    auto &customer_valid = customer_view._dataCols[cust_valid_i];
    auto &cnt_valid = cnt_view._dataCols[cnt_valid_i];

    std::vector<int64_t> in_mask = Views::in(customer_keys, cnt_keys, customer_valid, cnt_valid);

    // 2) 取反：NOT IN = IN ^ 1（只需一方异或 1 即可）
    std::vector<int64_t> one_mask(in_mask.size(), (Comm::rank() == 1 ? 1 : 0)); // 公共常量1的布尔分享
    auto not_in_mask =
            BoolXorBatchOperator(&in_mask, &one_mask,
                                 /*width=*/64,
                                 /*taskTag=*/0, /*msgTag=*/tid,
                                 SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

    // 3) new_valid = customer_valid AND not_in_mask
    auto zero_bucket_valid =
            BoolAndBatchOperator(&customer_valid, &not_in_mask,
                                 /*width=*/1,
                                 /*taskTag=*/0, /*msgTag=*/tid,
                                 SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

    // 4) 计数（密态）：Bool -> Arith -> 本地求和
    auto zero_valid_arith_vec =
            BoolToArithBatchOperator(&zero_bucket_valid,
                                     /*width=*/64,
                                     /*taskTag=*/0, /*msgTag=*/tid,
                                     SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

    unsigned long long acc = 0ULL;
    for (auto z: zero_valid_arith_vec) acc += (unsigned long long) z;
    int64_t zero_bucket_arith = (int64_t) acc;

    // 5) 算术 -> 布尔（得到 custdist 的布尔分享）
    std::vector<int64_t> one_elem_arith{zero_bucket_arith};
    auto zero_bucket_bool =
            ArithToBoolBatchOperator(&one_elem_arith,
                                     /*width=*/64,
                                     /*taskTag=*/0, /*msgTag=*/tid,
                                     SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis; // size=1

    // 6) 构造 zeros 视图：一行 (c_count=0, custdist=zero_bucket)
    std::vector<std::string> fields = {"c_count", "custdist"};
    std::vector<int> widths = {64, 64};
    std::string tname = "zeros";
    View zeros_view(tname, fields, widths);

    const int dataCols = zeros_view.colNum(); // 2
    const int validIdx = dataCols + View::VALID_COL_OFFSET;
    zeros_view._dataCols.resize(4);

    // c_count = 0（布尔分享：两端写0即可）
    zeros_view._dataCols[0].push_back(0);
    // custdist = zero_bucket（布尔分享）
    zeros_view._dataCols[1].push_back(zero_bucket_bool[0]);
    // valid 列：写 XOR 为 1 的分享（例如 rank1=1, rank0=0）
    zeros_view._dataCols[2].push_back(Comm::rank() == 1 ? 1 : 0);
    zeros_view._dataCols[3].push_back(0);

    return zeros_view;
}

// ---------- UNION ALL 操作 ----------
View unionHistAndZeros(View &hist_view, View &zeros_view, int tid) {
    // Create result view with same structure
    std::vector<std::string> fields = {"c_count", "custdist"};
    std::vector<int> widths = {64, 64};
    View result_view(fields, widths);

    // Initialize result view data columns
    result_view._dataCols.resize(hist_view._dataCols.size());

    // Copy hist data
    for (size_t i = 0; i < hist_view._dataCols.size(); ++i) {
        result_view._dataCols[i] = hist_view._dataCols[i];
    }

    // Append zeros data if it exists
    if (zeros_view.rowNum() > 0) {
        for (size_t i = 0; i < zeros_view._dataCols.size() && i < result_view._dataCols.size(); ++i) {
            result_view._dataCols[i].insert(result_view._dataCols[i].end(),
                                            zeros_view._dataCols[i].begin(),
                                            zeros_view._dataCols[i].end());
        }
    }

    return result_view;
}

// ---------- 最终排序：custdist DESC, c_count DESC ----------
View sortFinalResults(View &final_view, int tid) {
    if (final_view.rowNum() == 0) {
        return final_view;
    }

    std::vector<std::string> sortFields = {"custdist", "c_count"};
    std::vector<bool> sortOrders = {false, false}; // false = descending
    final_view.sort(sortFields, sortOrders, tid + 500);

    return final_view;
}

int main(int argc, char *argv[]) {
    System::init(argc, argv);
    DbConf::init();
    auto tid = System::nextTask() << (32 - Conf::TASK_TAG_BITS);

    int customer_rows = 14; // 建议用 14 和 24 来覆盖"多 block"
    int orders_rows = 24;
    if (Conf::_userParams.count("rows1")) customer_rows = std::stoi(Conf::_userParams["rows1"]);
    if (Conf::_userParams.count("rows2")) orders_rows = std::stoi(Conf::_userParams["rows2"]);

    if (Comm::isClient()) {
        Log::i("Optimized query data size: customer={}, orders={}", customer_rows, orders_rows);
    }

    // 1) 固定明文数据（含 block 复制/裁剪）
    std::vector<int64_t> c_keys, o_cust, o_okey, o_flag;
    generateTestData(customer_rows, orders_rows, c_keys, o_cust, o_okey, o_flag);

    // 3) 分享数据
    auto c_key_b = Secrets::boolShare(c_keys, 2, 64, tid);
    auto o_cust_b = Secrets::boolShare(o_cust, 2, 64, tid);
    auto o_okey_b = Secrets::boolShare(o_okey, 2, 64, tid);
    auto o_flag_b = Secrets::boolShare(o_flag, 2, 64, tid);

    // 4) 分享常量：word=0，用 XOR 分享（两端 share 相同）
    int64_t word;
    if (Comm::isClient()) {
        word = Math::randInt();
        int64_t s = Math::randInt();
        Comm::send(s, 64, 0, tid);
        Comm::send(word ^ s, 64, 1, tid); // s ^ s = 0
    } else {
        Comm::receive(word, 64, 2, tid);
    }

    if (Comm::isServer()) {
        auto vCustomer = createCustomerTable(c_key_b);
        auto vOrders = createOrdersTable(o_cust_b, o_okey_b, o_flag_b);

        auto t0 = System::currentTimeMillis();

        // Step 1: Create CTE cnt (filter and group by customer)
        auto vCnt = createCntCTE(vOrders, word, tid);
        auto vCnt_copy = vCnt;

        View vHist, vZeros;
        if (DbConf::BASELINE_MODE || DbConf::NO_COMPACTION) {
            // Step 2: Create CTE hist (group by count for c_count >= 1)
            vHist = createHistCTE(vCnt, tid);

            // Step 3: Create CTE zeros (calculate zero bucket)
            vZeros = createZerosCTE(vCustomer, vCnt_copy, tid);
        } else {
            auto f = ThreadPoolSupport::submit([&] {
                return createHistCTE(vCnt, tid);
            });
            vZeros = createZerosCTE(vCustomer, vCnt_copy, tid + 1000);
            vHist = f.get();
        }

        // Step 4: UNION ALL hist and zeros
        auto vUnion = unionHistAndZeros(vHist, vZeros, tid);

        auto vResult = sortFinalResults(vUnion, tid);
        Log::i("Execution time: {}ms", (System::currentTimeMillis() - t0));
    }

    System::finalize();
    return 0;
}
