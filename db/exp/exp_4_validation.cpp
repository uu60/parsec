//
// Created by 杜建璋 on 25-9-13.
//
#include <stdio.h>
#include <assert.h>
#include <sys/time.h>

#include "secret/Secrets.h"
#include "utils/System.h"

#include "../include/basis/View.h"
#include "../include/basis/Views.h"
#include "../include/basis/Table.h"
#include "../include/operator/SelectSupport.h"

#include "utils/Log.h"
#include "utils/Math.h"
#include "conf/DbConf.h"

// 仅用布尔域算子
#include "compute/batch/bool/BoolEqualBatchOperator.h"
#include "compute/batch/bool/BoolAndBatchOperator.h"

#include <string>
#include <vector>
#include <algorithm>

#include "utils/StringUtils.h"

// ---------- 前置声明 ----------
void generateTestData(int rows,
                      std::vector<int64_t> &id_data,
                      std::vector<int64_t> &pwd_data);

void generateFixedData(std::vector<int64_t> &ids,
                       std::vector<int64_t> &pwds);

View createRTable(std::vector<int64_t> &id_shares,
                  std::vector<int64_t> &pwd_shares);

// 排序 + 相邻比较：输出布尔位 having_bits（该行所在组 COUNT(*)>1）
std::vector<int64_t> markGroupsCountGT1(View &v, int tid);

// 计算“组首” heads：已排序条件下，heads[i]=1 表示 (ID,PWD)[i] 开启新组
std::vector<int64_t> buildGroupHeads(View &v, int tid);

// 取组首：valid = having_bits AND heads；仅保留 ID 列并压缩
View projectIdAndKeepGroupHeads(View &sorted_view,
                                std::vector<int64_t> &having_bits,
                                int tid);

// ---------- 主程序 ----------
int main(int argc, char *argv[]) {
    System::init(argc, argv);
    DbConf::init();
    auto tid = System::nextTask() << (32 - Conf::TASK_TAG_BITS);

    // 1) 固定明文数据（client 侧 rank==2）
    std::vector<int64_t> id_plain, pwd_plain;
    generateFixedData(id_plain, pwd_plain);

    // 2) 明文期望结果（client 打印）
    if (Comm::isClient()) {
        // 手算期望：每个 (ID,PWD) 组重复则吐出该组的 ID 一次
        std::vector<std::pair<int64_t,int64_t>> rows;
        for (size_t i=0;i<id_plain.size();++i) rows.emplace_back(id_plain[i], pwd_plain[i]);
        std::sort(rows.begin(), rows.end()); // 先排好方便数
        std::vector<int64_t> expected;
        for (size_t i=0;i<rows.size();) {
            size_t j=i+1;
            while (j<rows.size() && rows[j]==rows[i]) ++j;
            if (j-i>1) expected.push_back(rows[i].first); // 该组出现>1
            i=j;
        }
        Log::i("Expected IDs (one per (ID,PWD) group with COUNT>1): {}", StringUtils::vecToString(expected));
    }

    // 3) 转布尔份额
    auto id_shares  = Secrets::boolShare(id_plain,  2, 64, tid);
    auto pwd_shares = Secrets::boolShare(pwd_plain, 2, 64, tid);

    if (Comm::isServer()) {
        // 4) 建表
        auto r_view = createRTable(id_shares, pwd_shares);

        // 5) 计时：排序 + 相邻比较 + 组首过滤
        auto t0 = System::currentTimeMillis();

        // 5.1 排序 (ID ASC, PWD ASC)
        std::vector<std::string> order = {"ID","PWD"};
        std::vector<bool> asc = {true,true};
        r_view.sort(order, asc, tid+1);

        // 5.2 HAVING>1 的布尔位（标识整组的所有行）
        auto having_bits = markGroupsCountGT1(r_view, tid+10);

        // 5.3 仅保留“每个满足>1组的第一行”，并投影成单列 ID
        auto result = projectIdAndKeepGroupHeads(r_view, having_bits, tid+20);

        auto t1 = System::currentTimeMillis();
        Log::i("Validation run time: {}ms", (t1 - t0));

        // 6) 揭示并打印“实际输出”
        Views::revealAndPrint(result);
        // 你会看到一列 ID，应该与上面的 Expected IDs 完全一致（次序为 (ID,PWD) 排序下的组首顺序）
    }

    System::finalize();
    return 0;
}

// ---------- 实现 ----------
void generateTestData(int rows,
                      std::vector<int64_t> &id_data,
                      std::vector<int64_t> &pwd_data) {
    if (Comm::rank() == 2) {
        id_data.reserve(rows);
        pwd_data.reserve(rows);

        for (int i = 0; i < rows; i++) {
            id_data.push_back(Math::randInt());
            pwd_data.push_back(Math::randInt());
        }
    }
}

void generateFixedData(std::vector<int64_t> &ids,
                       std::vector<int64_t> &pwds) {
    if (Comm::rank()!=2) return;
    // 固定 16 行，可与日志比对
    int64_t ID[] =  {
        1,1,1,  2,2,  3,  4,4,4,  5,  6,6,  7,  8,8,  9
    };
    int64_t PWD[] = {
        10,10,20, 30,30, 40, 50,50,50, 60, 70,70, 80, 90,90, 100
    };
    int N = sizeof(ID)/sizeof(ID[0]);
    ids.assign(ID, ID+N);
    pwds.assign(PWD, PWD+N);
}

View createRTable(std::vector<int64_t> &id_shares,
                  std::vector<int64_t> &pwd_shares) {
    std::string table_name = "R";
    std::vector<std::string> fields = {"ID", "PWD"};
    std::vector<int> widths = {64, 64};

    Table r_table(table_name, fields, widths, "");
    for (size_t i=0;i<id_shares.size();++i) {
        r_table.insert({ id_shares[i], pwd_shares[i] });
    }
    return Views::selectAll(r_table);
}

// 在已排序视图上：对于每行 i，判断与 i-1 或 i+1 是否同 (ID,PWD) 组
std::vector<int64_t> markGroupsCountGT1(View &v, int tid) {
    int n = v.rowNum();
    std::vector<int64_t> zeros(n, 0);
    if (n==0) return zeros;

    int id_idx  = v.colIndex("ID");
    int pwd_idx = v.colIndex("PWD");
    auto &id_col  = v._dataCols[id_idx];
    auto &pwd_col = v._dataCols[pwd_idx];

    // eq_prev：与上一行相等（长度 n-1）
    std::vector<int64_t> id_prev(id_col.begin()+1, id_col.end());
    std::vector<int64_t> id_prev2(id_col.begin(),   id_col.end()-1);
    auto eq_id_prev = BoolEqualBatchOperator(&id_prev, &id_prev2, 64, 0,
                        tid+1, SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    std::vector<int64_t> pwd_prev(pwd_col.begin()+1, pwd_col.end());
    std::vector<int64_t> pwd_prev2(pwd_col.begin(),   pwd_col.end()-1);
    auto eq_pwd_prev = BoolEqualBatchOperator(&pwd_prev, &pwd_prev2, 64, 0,
                        tid+2, SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    auto same_prev = BoolAndBatchOperator(&eq_id_prev, &eq_pwd_prev, 1, 0,
                        tid+3, SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    std::vector<int64_t> prev_dup(n, 0);
    std::copy(same_prev.begin(), same_prev.end(), prev_dup.begin()+1);

    // eq_next：与下一行相等（长度 n-1）
    std::vector<int64_t> id_next(id_col.begin(),   id_col.end()-1);
    std::vector<int64_t> id_next2(id_col.begin()+1, id_col.end());
    auto eq_id_next = BoolEqualBatchOperator(&id_next, &id_next2, 64, 0,
                        tid+4, SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    std::vector<int64_t> pwd_next(pwd_col.begin(),   pwd_col.end()-1);
    std::vector<int64_t> pwd_next2(pwd_col.begin()+1, pwd_col.end());
    auto eq_pwd_next = BoolEqualBatchOperator(&pwd_next, &pwd_next2, 64, 0,
                        tid+5, SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    auto same_next = BoolAndBatchOperator(&eq_id_next, &eq_pwd_next, 1, 0,
                        tid+6, SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    std::vector<int64_t> next_dup(n, 0);
    std::copy(same_next.begin(), same_next.end(), next_dup.begin());

    // OR = XOR ^ AND
    std::vector<int64_t> axb(n);
    for (int i=0;i<n;++i) axb[i] = prev_dup[i] ^ next_dup[i];
    auto aandb = BoolAndBatchOperator(&prev_dup, &next_dup, 1, 0,
                        tid+7, SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    for (int i=0;i<n;++i) axb[i] ^= aandb[i];

    return axb; // having_bits
}

std::vector<int64_t> buildGroupHeads(View &v, int tid) {
    int n = v.rowNum();
    std::vector<int64_t> heads(n, 0);
    if (n==0) return heads;

    int id_idx  = v.colIndex("ID");
    int pwd_idx = v.colIndex("PWD");
    auto &id_col  = v._dataCols[id_idx];
    auto &pwd_col = v._dataCols[pwd_idx];

    // 与上一行是否同组 same_prev（长度 n-1）
    std::vector<int64_t> id_prev(id_col.begin()+1, id_col.end());
    std::vector<int64_t> id_prev2(id_col.begin(),   id_col.end()-1);
    auto eq_id_prev = BoolEqualBatchOperator(&id_prev, &id_prev2, 64, 0,
                        tid+11, SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    std::vector<int64_t> pwd_prev(pwd_col.begin()+1, pwd_col.end());
    std::vector<int64_t> pwd_prev2(pwd_col.begin(),   pwd_col.end()-1);
    auto eq_pwd_prev = BoolEqualBatchOperator(&pwd_prev, &pwd_prev2, 64, 0,
                        tid+12, SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    auto same_prev = BoolAndBatchOperator(&eq_id_prev, &eq_pwd_prev, 1, 0,
                        tid+13, SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

    // heads[0]=1（在 XOR 分享下仅 rank==0 侧写 1）
    if (Comm::rank()==0) heads[0] = 1;
    // heads[i] = NOT( same_prev[i-1] )
    heads.resize(n,0);
    for (int i=1;i<n;++i) heads[i] = same_prev[i-1];
    if (Comm::rank()==0) {
        for (int i=1;i<n;++i) heads[i] ^= 1; // 取反：仅一侧 ^1
    }
    return heads;
}

View projectIdAndKeepGroupHeads(View &sorted_view,
                                std::vector<int64_t> &having_bits,
                                int tid) {
    // 组首
    auto heads = buildGroupHeads(sorted_view, tid+30);

    // valid = heads AND having_bits  => 每个重复组仅留第一行
    auto valid = BoolAndBatchOperator(&heads, &having_bits, 1, 0,
                 tid+31, SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

    // 仅保留 ID 列
    std::vector<std::string> fields = {"ID"};
    std::vector<int> widths = {64};
    View out(fields, widths);
    out._dataCols[0] = sorted_view._dataCols[ sorted_view.colIndex("ID") ];
    out._dataCols[out.colNum()+View::VALID_COL_OFFSET]   = std::move(valid);
    out._dataCols[out.colNum()+View::PADDING_COL_OFFSET] = std::vector<int64_t>(out._dataCols[0].size(), 0);

    out.clearInvalidEntries(tid+32);
    return out;
}