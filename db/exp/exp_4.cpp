
#include "secret/Secrets.h"
#include "utils/System.h"

#include "../include/basis/View.h"
#include "../include/basis/Views.h"
#include "../include/basis/Table.h"
#include "../include/operator/SelectSupport.h"

#include "utils/Log.h"
#include "utils/Math.h"
#include "conf/DbConf.h"

#include "compute/batch/bool/BoolEqualBatchOperator.h"
#include "compute/batch/bool/BoolAndBatchOperator.h"

#include <string>
#include <vector>
#include <algorithm>

#include "parallel/ThreadPoolSupport.h"
#include "utils/StringUtils.h"

void generateTestData(int rows,
                      std::vector<int64_t> &id_data,
                      std::vector<int64_t> &pwd_data);

View createRTable(std::vector<int64_t> &id_shares,
                  std::vector<int64_t> &pwd_shares);

std::vector<int64_t> markGroupsCountGT1(View &v, int tid);

std::vector<int64_t> buildGroupHeads(View &v, int tid);

View projectIdAndKeepGroupHeads(View &sorted_view,
                                std::vector<int64_t> &having_bits,
                                int tid);

int main(int argc, char *argv[]) {
    System::init(argc, argv);
    DbConf::init();
    auto tid = System::nextTask() << (32 - Conf::TASK_TAG_BITS);

    int rows = 1000;
    if (Conf::_userParams.count("rows")) {
        rows = std::stoi(Conf::_userParams["rows"]);
    }

    if (Comm::isClient()) {
        Log::i("Data size: R: {}", rows);
    }

    std::vector<int64_t> id_plain, pwd_plain;
    generateTestData(rows, id_plain, pwd_plain);

    auto id_shares = Secrets::boolShare(id_plain, 2, 64, tid);
    auto pwd_shares = Secrets::boolShare(pwd_plain, 2, 64, tid);

    if (Comm::isServer()) {
        auto r_view = createRTable(id_shares, pwd_shares);

        auto t0 = System::currentTimeMillis();

        std::vector<std::string> order = {"ID", "PWD"};
        std::vector<bool> asc = {true, true};
        r_view.sort(order, asc, tid);

        auto having_bits = markGroupsCountGT1(r_view, tid);

        auto result = projectIdAndKeepGroupHeads(r_view, having_bits, tid);

        auto t1 = System::currentTimeMillis();
        Log::i("Execution time: {}ms", (t1 - t0));
    }

    System::finalize();
    return 0;
}

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

View createRTable(std::vector<int64_t> &id_shares,
                  std::vector<int64_t> &pwd_shares) {
    std::string table_name = "R";
    std::vector<std::string> fields = {"ID", "PWD"};
    std::vector<int> widths = {64, 64};

    Table r_table(table_name, fields, widths, "");
    for (size_t i = 0; i < id_shares.size(); ++i) {
        r_table.insert({id_shares[i], pwd_shares[i]});
    }
    return Views::selectAll(r_table);
}

std::vector<int64_t> markGroupsCountGT1(View &v, int tid) {
    int n = v.rowNum();
    std::vector<int64_t> zeros(n, 0);
    if (n == 0) return zeros;

    int id_idx = v.colIndex("ID");
    int pwd_idx = v.colIndex("PWD");
    auto &id_col = v._dataCols[id_idx];
    auto &pwd_col = v._dataCols[pwd_idx];

    std::vector<int64_t> id_prev(id_col.begin() + 1, id_col.end());
    std::vector<int64_t> id_prev2(id_col.begin(), id_col.end() - 1);

    std::vector<int64_t> eq_id_prev, eq_pwd_prev;
    if (DbConf::BASELINE_MODE) {
        eq_id_prev = BoolEqualBatchOperator(&id_prev, &id_prev2, 64, 0,
                                            tid, SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        std::vector<int64_t> pwd_prev(pwd_col.begin() + 1, pwd_col.end());
        std::vector<int64_t> pwd_prev2(pwd_col.begin(), pwd_col.end() - 1);
        eq_pwd_prev = BoolEqualBatchOperator(&pwd_prev, &pwd_prev2, 64, 0,
                                             tid, SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    } else {
        auto f = ThreadPoolSupport::submit([&] {
            return BoolEqualBatchOperator(&id_prev, &id_prev2, 64, 0,
                                          tid + BoolEqualBatchOperator::tagStride(),
                                          SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        });
        std::vector<int64_t> pwd_prev(pwd_col.begin() + 1, pwd_col.end());
        std::vector<int64_t> pwd_prev2(pwd_col.begin(), pwd_col.end() - 1);
        eq_pwd_prev = BoolEqualBatchOperator(&pwd_prev, &pwd_prev2, 64, 0,
                                             tid, SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        eq_id_prev = std::move(f.get());
    }

    auto same_prev = BoolAndBatchOperator(&eq_id_prev, &eq_pwd_prev, 1, 0,
                                          tid, SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    std::vector<int64_t> prev_dup(n, 0);
    std::copy(same_prev.begin(), same_prev.end(), prev_dup.begin() + 1);

    std::vector<int64_t> id_next(id_col.begin(), id_col.end() - 1);
    std::vector<int64_t> id_next2(id_col.begin() + 1, id_col.end());

    std::vector<int64_t> eq_id_next, eq_pwd_next;
    if (DbConf::BASELINE_MODE) {
        eq_id_next = BoolEqualBatchOperator(&id_next, &id_next2, 64, 0,
                                            tid, SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        std::vector<int64_t> pwd_next(pwd_col.begin(), pwd_col.end() - 1);
        std::vector<int64_t> pwd_next2(pwd_col.begin() + 1, pwd_col.end());
        eq_pwd_next = BoolEqualBatchOperator(&pwd_next, &pwd_next2, 64, 0,
                                             tid, SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    } else {
        auto f = ThreadPoolSupport::submit([&] {
            return BoolEqualBatchOperator(&id_next, &id_next2, 64, 0,
                                          tid + BoolEqualBatchOperator::tagStride(),
                                          SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        });
        std::vector<int64_t> pwd_next(pwd_col.begin(), pwd_col.end() - 1);
        std::vector<int64_t> pwd_next2(pwd_col.begin() + 1, pwd_col.end());
        eq_pwd_next = BoolEqualBatchOperator(&pwd_next, &pwd_next2, 64, 0,
                                             tid, SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        eq_id_next = std::move(f.get());
    }

    auto same_next = BoolAndBatchOperator(&eq_id_next, &eq_pwd_next, 1, 0,
                                          tid, SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    std::vector<int64_t> next_dup(n, 0);
    std::copy(same_next.begin(), same_next.end(), next_dup.begin());

    std::vector<int64_t> axb(n);
    for (int i = 0; i < n; ++i) axb[i] = prev_dup[i] ^ next_dup[i];
    auto aandb = BoolAndBatchOperator(&prev_dup, &next_dup, 1, 0,
                                      tid, SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    for (int i = 0; i < n; ++i) axb[i] ^= aandb[i];

    return axb;
}

std::vector<int64_t> buildGroupHeads(View &v, int tid) {
    int n = v.rowNum();
    std::vector<int64_t> heads(n, 0);
    if (n == 0) return heads;

    int id_idx = v.colIndex("ID");
    int pwd_idx = v.colIndex("PWD");
    auto &id_col = v._dataCols[id_idx];
    auto &pwd_col = v._dataCols[pwd_idx];

    std::vector<int64_t> eq_id_prev, eq_pwd_prev;
    if (DbConf::BASELINE_MODE) {
        std::vector<int64_t> id_prev(id_col.begin() + 1, id_col.end());
        std::vector<int64_t> id_prev2(id_col.begin(), id_col.end() - 1);
        eq_id_prev = BoolEqualBatchOperator(&id_prev, &id_prev2, 64, 0,
                                            tid, SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        std::vector<int64_t> pwd_prev(pwd_col.begin() + 1, pwd_col.end());
        std::vector<int64_t> pwd_prev2(pwd_col.begin(), pwd_col.end() - 1);
        eq_pwd_prev = BoolEqualBatchOperator(&pwd_prev, &pwd_prev2, 64, 0,
                                             tid, SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    } else {
        auto f = ThreadPoolSupport::submit([&] {
            std::vector<int64_t> id_prev(id_col.begin() + 1, id_col.end());
            std::vector<int64_t> id_prev2(id_col.begin(), id_col.end() - 1);
            return BoolEqualBatchOperator(&id_prev, &id_prev2, 64, 0,
                                          tid + BoolEqualBatchOperator::tagStride(),
                                          SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        });
        std::vector<int64_t> pwd_prev(pwd_col.begin() + 1, pwd_col.end());
        std::vector<int64_t> pwd_prev2(pwd_col.begin(), pwd_col.end() - 1);
        eq_pwd_prev = BoolEqualBatchOperator(&pwd_prev, &pwd_prev2, 64, 0,
                                             tid, SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        eq_id_prev = std::move(f.get());
    }

    auto same_prev = BoolAndBatchOperator(&eq_id_prev, &eq_pwd_prev, 1, 0,
                                          tid, SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

    if (Comm::rank() == 0) heads[0] = 1;
    heads.resize(n, 0);
    for (int i = 1; i < n; ++i) heads[i] = same_prev[i - 1];
    if (Comm::rank() == 0) {
        for (int i = 1; i < n; ++i) heads[i] ^= 1;
    }
    return heads;
}

View projectIdAndKeepGroupHeads(View &sorted_view,
                                std::vector<int64_t> &having_bits,
                                int tid) {
    auto heads = buildGroupHeads(sorted_view, tid);

    auto valid = BoolAndBatchOperator(&heads, &having_bits, 1, 0,
                                      tid, SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

    std::vector<std::string> fields = {"ID"};
    std::vector<int> widths = {64};
    View out(fields, widths);
    out._dataCols[0] = sorted_view._dataCols[sorted_view.colIndex("ID")];
    out._dataCols[out.colNum() + View::VALID_COL_OFFSET] = std::move(valid);
    out._dataCols[out.colNum() + View::PADDING_COL_OFFSET] = std::vector<int64_t>(out._dataCols[0].size(), 0);
    return out;
}
