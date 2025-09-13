// Validation version for TPCH Q13 (custdist)
// 固定数据 + 期望输出
//
// SELECT c_count, count(*) AS custdist
// FROM (
//   SELECT c_custkey, count(o_orderkey) AS c_count
//   FROM customer LEFT OUTER JOIN orders
//     ON c_custkey = o_custkey AND o_comment_flag = 0
//   GROUP BY c_custkey
// ) AS c_orders(c_custkey, c_count)
// GROUP BY c_count
// ORDER BY custdist DESC, c_count DESC

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

// ---------- 固定数据模式（一个 block） ----------
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

// 生成固定 block，并按需重复/裁剪至请求规模；保证每个 block 的键域独立
static void generateFixedDataRepeated(
    int customer_rows, int orders_rows,
    std::vector<int64_t> &c_keys,
    std::vector<int64_t> &o_cust,
    std::vector<int64_t> &o_okey,
    std::vector<int64_t> &o_flag) {
    if (Comm::rank() != 2) return;

    // --- base customers (7) ---
    int BASE_C = 7;
    std::vector<int64_t> baseCust = {1, 2, 3, 4, 5, 6, 7};

    // --- base orders (12)，引用上面的 1..7 ---
    // flag=0: 保留；flag=1: 排除（等价于 o_comment not like ...）
    std::vector<OrderRow> baseOrders = {
        {1, 101, 0}, {1, 102, 0},
        {2, 201, 0},
        {3, 301, 1}, {3, 302, 1},
        {5, 501, 0}, {5, 502, 1}, {5, 503, 0},
        {6, 601, 0}, {6, 602, 0}, {6, 603, 0},
        {7, 701, 1}
    };

    // 计算需要多少个 block
    int blocks_c = (customer_rows + BASE_C - 1) / BASE_C;
    int blocks_o = (orders_rows + (int) baseOrders.size() - 1) / (int) baseOrders.size();
    int blocks = std::max(blocks_c, blocks_o);

    c_keys.reserve(blocks * BASE_C);
    int64_t orderkey_seed = 1;

    for (int b = 0; b < blocks; ++b) {
        int64_t coff = (int64_t) b * BASE_C; // 每个 block 的客户键偏移
        // customers
        for (int i = 0; i < BASE_C; ++i) {
            int64_t ck = baseCust[i] + coff;
            c_keys.push_back(ck);
        }
        // orders
        for (auto r: baseOrders) {
            int64_t ck = r.cust + coff;
            int64_t ok = orderkey_seed++; // 仅保证唯一
            o_cust.push_back(ck);
            o_okey.push_back(ok);
            o_flag.push_back(r.flag);
        }
    }

    // 裁剪到目标行数
    if ((int) c_keys.size() > customer_rows) {
        c_keys.resize(customer_rows);
    }
    if ((int) o_cust.size() > orders_rows) {
        o_cust.resize(orders_rows);
        o_okey.resize(orders_rows);
        o_flag.resize(orders_rows);
    }
}

// 明文推导期望结果（基于上面生成的同一份明文）
static std::vector<std::pair<int64_t, int64_t> > computeExpected(
    std::vector<int64_t> &c_keys,
    std::vector<int64_t> &o_cust,
    std::vector<int64_t> &o_flag) {
    // 每个客户的计数（只统计 flag==0 的订单）
    std::map<int64_t, int64_t> perCustomer;
    for (auto ck: c_keys) perCustomer[ck] = 0;
    for (size_t i = 0; i < o_cust.size(); ++i) {
        int64_t ck = o_cust[i];
        auto it = perCustomer.find(ck);
        if (it != perCustomer.end() && o_flag[i] == 0) it->second += 1;
    }

    // 直方图：c_count -> custdist
    std::map<int64_t, int64_t> hist;
    for (auto &kv: perCustomer) hist[kv.second]++;

    // 转为 vector 并排序：custdist desc, c_count desc
    std::vector<std::pair<int64_t, int64_t> > out;
    out.reserve(hist.size());
    for (auto &kv: hist) out.emplace_back(kv.first, kv.second);
    std::sort(out.begin(), out.end(),
              [](auto &a, auto &b) {
                  if (a.second != b.second) return a.second > b.second; // custdist desc
                  return a.first > b.first; // c_count desc
              });
    return out;
}

// ---------- 你原有的构建/运算函数（小改动：构造零常量、打印期望） ----------

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

// o_comment_flag == word（此处 word=0）
View filterOrdersByComment(View &orders_view, int64_t word, int tid) {
    std::vector<std::string> fields = {"o_comment_flag"};
    std::vector<View::ComparatorType> cmps = {View::EQUALS};
    std::vector<int64_t> consts = {word};
    View v = orders_view;
    v.filterAndConditions(fields, cmps, consts, false, tid);
    return v;
}

// ====== 新增：orders 侧先聚合：GROUP BY o_custkey → count(*) AS c_count ======
View groupOrdersByCustomerCount(View &filtered_orders_view, int tid) {
    std::vector<std::string> group_fields = {"o_custkey"};
    // 只保留分组所需列
    filtered_orders_view.select(group_fields);

    // 分组边界
    auto gb = filtered_orders_view.groupBy("o_custkey", tid + 2);

    // 对每个 o_custkey 计数：count(*) -> 列名 c_count
    filtered_orders_view.count(group_fields, gb, "c_count", tid + 3);

    // 此时列为：o_custkey, c_count （每个 o_custkey 一行）
    return filtered_orders_view;
}

// ====== 新增：把右表聚合结果左合并到 customer 上，得到 (c_custkey, c_count) ======
View attachCountsToCustomers(View &customer_view, View &ocnt_view, int tid) {
    std::string ck = "c_custkey";
    std::string ok = "o_custkey";

    // 左外连接（保持“无匹配客户”）
    View joined = Views::leftOuterJoin(customer_view, ocnt_view, ck, ok, false, false);
    joined.clearInvalidEntries(tid);

    std::vector<std::string> field_names = {"customer.c_custkey", "orders.c_count"};
    // 只保留需要的两列：customer.c_custkey 与 orders.c_count
    joined.select(field_names);

    // 规范字段名（若不支持 rename，可直接改 _fieldNames）
    if (!joined._fieldNames.empty()) {
        joined._fieldNames[0] = "c_custkey";
        if (joined._fieldNames.size() > 1) joined._fieldNames[1] = "c_count";
    }
    return joined;
}

// 第二次分组：对 c_count 做直方图（custdist）
View groupByCountToCustdist(View &cc_view, int tid) {
    std::vector<std::string> group_fields = {"c_count"};
    // 只保留 c_count
    cc_view.select(group_fields);

    // 分组并计数 -> custdist
    auto gb = cc_view.groupBy("c_count", tid + 21);
    cc_view.count(group_fields, gb, "custdist", tid + 22);

    // 现在表结构是：c_count, custdist
    return cc_view;
}

// 最终排序：custdist desc, c_count desc
View sortFinalResults(View &final_view, int tid) {
    std::vector<std::string> sortFields = {"custdist", "c_count"};
    std::vector<bool> sortOrders = {false, false}; // false = descending
    final_view.sort(sortFields, sortOrders, tid + 30);
    return final_view;
}

int main(int argc, char *argv[]) {
    System::init(argc, argv);
    DbConf::init();
    auto tid = System::nextTask() << (32 - Conf::TASK_TAG_BITS);

    int customer_rows = 14; // 建议用 14 和 24 来覆盖“多 block”
    int orders_rows = 24;
    if (Conf::_userParams.count("rows1")) customer_rows = std::stoi(Conf::_userParams["rows1"]);
    if (Conf::_userParams.count("rows2")) orders_rows = std::stoi(Conf::_userParams["rows2"]);

    if (Comm::isClient()) Log::i("Validation data size: customer={}, orders={}", customer_rows, orders_rows);

    // 1) 固定明文数据（含 block 复制/裁剪）
    std::vector<int64_t> c_keys, o_cust, o_okey, o_flag;
    generateFixedDataRepeated(customer_rows, orders_rows, c_keys, o_cust, o_okey, o_flag);

    // 2) 明文端计算并打印期望结果（客户端）
    if (Comm::rank() == 2) {
        auto expected = computeExpected(c_keys, o_cust, o_flag);
        std::vector<std::string> rows;
        rows.reserve(expected.size());
        for (auto &p: expected) rows.push_back("(" + std::to_string(p.first) + ", " + std::to_string(p.second) + ")");
        Log::i("[EXPECTED] (c_count, custdist) sorted: {}", StringUtils::vecToString(rows));
    }

    // 3) 分享数据
    auto c_key_b = Secrets::boolShare(c_keys, 2, 64, tid);
    auto o_cust_b = Secrets::boolShare(o_cust, 2, 64, tid);
    auto o_okey_b = Secrets::boolShare(o_okey, 2, 64, tid);
    auto o_flag_b = Secrets::boolShare(o_flag, 2, 64, tid);

    // 4) 分享常量：word=0，用 XOR 分享（两端 share 相同）
    int64_t word = 0;
    if (Comm::isClient()) {
        int64_t s = Math::randInt();
        Comm::send(s, 64, 0, tid);
        Comm::send(s, 64, 1, tid); // s ^ s = 0
    } else {
        Comm::receive(word, 64, 2, tid);
    }

    if (Comm::isServer()) {
        auto vC = createCustomerTable(c_key_b);
        auto vO = createOrdersTable(o_cust_b, o_okey_b, o_flag_b);

        auto t0 = System::currentTimeMillis();

        // ① 过滤 comment（flag==0）
        auto vOf = filterOrdersByComment(vO, word, tid + 100);
        auto t1 = System::currentTimeMillis();
        Log::i("Filter orders time: {}ms", (t1 - t0));

        // ② orders 侧先 group-by 计数：o_custkey -> c_count
        auto vOCnt = groupOrdersByCustomerCount(vOf, tid + 150);
        auto t2 = System::currentTimeMillis();
        Log::i("Group orders by customer count time: {}ms", (t2 - t1));

        // ③ 把计数左合并到 customer（无匹配客户自动得到 c_count=0）
        auto vCC  = attachCountsToCustomers(vC, vOCnt, tid + 200);
        auto t3 = System::currentTimeMillis();
        Log::i("Attach counts to customers time: {}ms", (t3 - t2));

        // ④ 对 c_count 做直方图：c_count -> custdist
        auto vHist = groupByCountToCustdist(vCC, tid + 300);
        auto t4 = System::currentTimeMillis();
        Log::i("Group by count to custdist time: {}ms", (t4 - t3));

        // ⑤ 最终排序
        auto vOut = sortFinalResults(vHist, tid + 400);

        // 展示实际结果
        Log::i("[ACTUAL ] (c_count, custdist) sorted:");
        Views::revealAndPrint(vOut);

        Log::i("Validation query time: {}ms", (System::currentTimeMillis() - t0));
    }

    System::finalize();
    return 0;
}