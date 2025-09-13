//
// Created by 杜建璋 on 25-8-16.
//
// COUNT(DISTINCT pid) via per-pid MIN/MAX + single compare （无 Secrecy/MPI 内容）
// —— 固定数据 + 期望结果（正确性验证版）
//
// SQL:
// SELECT count(DISTINCT pid)
// FROM diagnosis d JOIN medication m USING(pid)
// WHERE d.diag = hd AND m.med = aspirin AND d.time <= m.time
//
// 物理计划：
//   d' = filter(d, diag==hd)
//   m' = filter(m,  med==aspirin)
//   d_min = sort(d' by pid asc, $valid desc, time asc)  -> 组首且$valid
//   m_max = sort(m' by pid asc, $valid desc, time desc) -> 组首且$valid
//   J = hashJoin(d_min, m_max) on pid
//   valid = NOT(m.time < d.time)   // 即 d.time <= m.time
//   clearInvalidEntries()
//   rows(J) == 满足条件的 DISTINCT pid 数
//

#include "secret/Secrets.h"
#include "utils/System.h"

#include "../include/basis/View.h"
#include "../include/basis/Views.h"
#include "../include/basis/Table.h"
#include "../include/operator/SelectSupport.h"

#include "utils/Log.h"
#include "utils/Math.h"

#include "compute/batch/bool/BoolLessBatchOperator.h"
#include "compute/batch/bool/BoolEqualBatchOperator.h"
#include "compute/batch/bool/BoolAndBatchOperator.h"

#include "parallel/ThreadPoolSupport.h"
#include "conf/DbConf.h"

#include <string>
#include <vector>
#include <algorithm>

// -------------------- 固定测试数据（正确性验证） --------------------
// 期望命中的 DISTINCT pid 集合：{1, 2, 4, 6}，期望计数：4
// diagnosis(16 行): pid, diag, time
// medication(11 行): pid, med, time
// 诊断用 hd=1001；用药中 aspirin=2001

static int64_t kHD = Math::randInt();
static int64_t kASP = Math::randInt();

void generateTestData(int diagRows, int medRows,
                      std::vector<int64_t> &diagnosis_pid_data,
                      std::vector<int64_t> &diagnosis_diag_data,
                      std::vector<int64_t> &diagnosis_time_data,
                      std::vector<int64_t> &diagnosis_tag_data,
                      std::vector<int64_t> &medication_pid_data,
                      std::vector<int64_t> &medication_med_data,
                      std::vector<int64_t> &medication_time_data,
                      std::vector<int64_t> &medication_tag_data);

View createDiagnosisTable(std::vector<int64_t> &pid,
                          std::vector<int64_t> &diag,
                          std::vector<int64_t> &time,
                          std::vector<int64_t> &tag);

View createMedicationTable(std::vector<int64_t> &pid,
                           std::vector<int64_t> &med,
                           std::vector<int64_t> &time,
                           std::vector<int64_t> &tag);

View filterEquals(View v, std::string &col, int64_t secret_share, int tid);

// 每 pid 留一行：诊断保留 MIN(time)，用药保留 MAX(time)
View perPidMinOrMax(View v, bool keepMinTime, int tid);

// 以 NOT(rhs<lhs) 构造 (lhs <= rhs)（仅一次 BoolLess）
std::vector<int64_t> build_leq_bits(std::vector<int64_t> &lhs,
                                    std::vector<int64_t> &rhs,
                                    int baseTid);

// 从 join 后的视图中定位左右 time 列
void find_time_cols(View &joined, int &left_time_idx, int &right_time_idx);

// -------------------- 主流程 --------------------
int main(int argc, char *argv[]) {
    System::init(argc, argv);
    DbConf::init();
    auto tid = System::nextTask() << (32 - Conf::TASK_TAG_BITS);

    int diagRows = 1000, medRows = 1000;
    if (Conf::_userParams.count("rows1")) {
        diagRows = std::stoi(Conf::_userParams["rows1"]);
    }
    if (Conf::_userParams.count("rows2")) {
        medRows = std::stoi(Conf::_userParams["rows2"]);
    }
    if (Comm::isClient()) {
        Log::i("Data size: diagnosis: {} medication: {}", diagRows, medRows);
    }
    // 固定数据大小（会被 generateFixedTestData 覆盖为 16/11）

    // 固定明文（client 侧 rank==2）
    std::vector<int64_t> d_pid, d_diag, d_time, d_tag;
    std::vector<int64_t> m_pid, m_med, m_time, m_tag;
    generateTestData(diagRows, medRows, d_pid, d_diag, d_time, d_tag, m_pid, m_med, m_time, m_tag);

    // 转换为布尔份额
    auto d_pid_s = Secrets::boolShare(d_pid, 2, 64, tid);
    auto d_diag_s = Secrets::boolShare(d_diag, 2, 64, tid);
    auto d_time_s = Secrets::boolShare(d_time, 2, 64, tid);
    auto d_tag_s = Secrets::boolShare(d_tag, 2, 64, tid);

    auto m_pid_s = Secrets::boolShare(m_pid, 2, 64, tid);
    auto m_med_s = Secrets::boolShare(m_med, 2, 64, tid);
    auto m_time_s = Secrets::boolShare(m_time, 2, 64, tid);
    auto m_tag_s = Secrets::boolShare(m_tag, 2, 64, tid);

    // 秘密常量：构造成 kHD / kASP 的份额（c0 随机，c1 = c0 ^ CONST）
    int64_t hd_share, asp_share;
    if (Comm::isClient()) {
        int64_t r0 = Math::randInt();
        int64_t r1 = Math::randInt();
        // server0 收 r0, server1 收 r0^kHD -> 重构为 kHD
        Comm::send(r0, 64, 0, tid);
        Comm::send(r0 ^ kHD, 64, 1, tid);
        // server0 收 r1, server1 收 r1^kASP -> 重构为 kASP
        Comm::send(r1, 64, 0, tid);
        Comm::send(r1 ^ kASP, 64, 1, tid);
    } else {
        // 每个 server 各收自己的那一份
        Comm::receive(hd_share, 64, 2, tid);
        Comm::receive(asp_share, 64, 2, tid);
    }

    if (Comm::isServer()) {
        // 建表
        auto diagnosis_view = createDiagnosisTable(d_pid_s, d_diag_s, d_time_s, d_tag_s);
        auto medication_view = createMedicationTable(m_pid_s, m_med_s, m_time_s, m_tag_s);

        std::string col_diag = "diag";
        std::string col_med = "med";

        View d_min, m_max;
        auto t0 = System::currentTimeMillis();
        if (DbConf::BASELINE_MODE) {
            // 谓词下推
            auto dv = filterEquals(diagnosis_view, col_diag, hd_share, tid);
            auto mv = filterEquals(medication_view, col_med, asp_share, tid);

            // 每 pid 一行（d: MIN(time)，m: MAX(time)）
            d_min = perPidMinOrMax(dv, /*keepMinTime=*/true, tid);
            m_max = perPidMinOrMax(mv, /*keepMinTime=*/false, tid);
        } else {
            auto f = ThreadPoolSupport::submit([&] {
                auto dv = filterEquals(diagnosis_view, col_diag, hd_share, tid);
                auto d_min = perPidMinOrMax(dv, /*keepMinTime=*/true, tid);
                return d_min;
            });
            auto mv = filterEquals(medication_view, col_med, asp_share, tid + 1000);
            m_max = perPidMinOrMax(mv, /*keepMinTime=*/false, tid + 1000);

            d_min = f.get();
        }


        std::string field0 = "pid";
        // 小表 hashJoin 对齐 pid
        auto joined = Views::nestedLoopJoin(d_min, m_max, field0, field0, false);

        // 定位左右 time 列
        int d_time_idx = -1, m_time_idx = -1;
        find_time_cols(joined, d_time_idx, m_time_idx);

        // d.time <= m.time  ： NOT(m.time < d.time)
        auto &d_time_col = joined._dataCols[d_time_idx];
        auto &m_time_col = joined._dataCols[m_time_idx];
        auto le_bits = build_leq_bits(d_time_col, m_time_col, tid);

        auto valids = BoolAndBatchOperator(&le_bits, &joined._dataCols[joined.colNum() + View::VALID_COL_OFFSET],
                                           1, 0, tid,
                                           SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        // 应用有效位并压缩
        int valid_idx = joined.colNum() + View::VALID_COL_OFFSET;
        joined._dataCols[valid_idx] = std::move(valids);

        Log::i("Total query execution time: {}ms", System::currentTimeMillis() - t0);
    }

    System::finalize();
    return 0;
}

// -------------------- 实现 --------------------
void generateTestData(int diagRows, int medRows,
                      std::vector<int64_t> &diagnosis_pid_data,
                      std::vector<int64_t> &diagnosis_diag_data,
                      std::vector<int64_t> &diagnosis_time_data,
                      std::vector<int64_t> &diagnosis_tag_data,
                      std::vector<int64_t> &medication_pid_data,
                      std::vector<int64_t> &medication_med_data,
                      std::vector<int64_t> &medication_time_data,
                      std::vector<int64_t> &medication_tag_data) {
    if (Comm::rank() == 2) {
        // Generate diagnosis data
        diagnosis_pid_data.reserve(diagRows);
        diagnosis_diag_data.reserve(diagRows);
        diagnosis_time_data.reserve(diagRows);
        diagnosis_tag_data.reserve(diagRows);

        for (int i = 0; i < diagRows; i++) {
            int64_t pid = Math::randInt();
            int64_t diag = Math::randInt();
            int64_t time = Math::randInt();

            diagnosis_pid_data.push_back(pid);
            diagnosis_diag_data.push_back(diag);
            diagnosis_time_data.push_back(time);

            // Compute bucket tag using hash of the pid (join key)
            diagnosis_tag_data.push_back(Views::hash(pid));
        }

        // Generate medication data
        medication_pid_data.reserve(medRows);
        medication_med_data.reserve(medRows);
        medication_time_data.reserve(medRows);
        medication_tag_data.reserve(medRows);

        for (int i = 0; i < medRows; i++) {
            int64_t pid = Math::randInt();
            int64_t med = Math::randInt();
            int64_t time = Math::randInt();

            medication_pid_data.push_back(pid);
            medication_med_data.push_back(med);
            medication_time_data.push_back(time);

            // Compute bucket tag using hash of the pid (join key)
            medication_tag_data.push_back(Views::hash(pid));
        }
    }
}

View createDiagnosisTable(std::vector<int64_t> &pid,
                          std::vector<int64_t> &diag,
                          std::vector<int64_t> &time,
                          std::vector<int64_t> &tag) {
    std::string name = "diagnosis";
    std::vector<std::string> fields = {"pid", "diag", "time"};
    std::vector<int> widths = {64, 64, 64};
    Table t(name, fields, widths, "pid");
    for (size_t i = 0; i < pid.size(); ++i) {
        t.insert({pid[i], diag[i], time[i], tag[i]}); // 追加 $tag:pid
    }
    return Views::selectAll(t);
}

View createMedicationTable(std::vector<int64_t> &pid,
                           std::vector<int64_t> &med,
                           std::vector<int64_t> &time,
                           std::vector<int64_t> &tag) {
    std::string name = "medication";
    std::vector<std::string> fields = {"pid", "med", "time"};
    std::vector<int> widths = {64, 64, 64};
    Table t(name, fields, widths, "pid");
    for (size_t i = 0; i < pid.size(); ++i) {
        t.insert({pid[i], med[i], time[i], tag[i]});
    }
    return Views::selectAll(t);
}

View filterEquals(View v, std::string &col, int64_t secret_share, int tid) {
    std::vector<std::string> field_names = {col};
    std::vector<View::ComparatorType> comparator_types = {View::EQUALS};
    std::vector<int64_t> const_shares = {secret_share};
    v.filterAndConditions(field_names, comparator_types, const_shares, false, tid);
    return v;
}

// 不使用 groupBy；直接基于排好序的相邻比较构造“组首位”
View perPidMinOrMax(View v, bool keepMinTime, int tid) {
    // 1) 排序：pid 升序；$valid 降序；time 升/降（决定 MIN/MAX）
    std::vector<std::string> order = {"pid", "$valid", "time"};
    std::vector<bool> asc = {true, false, keepMinTime ? true : false};
    v.sort(order, asc, tid);

    std::string grp_col = "pid";
    auto heads = v.groupBy(grp_col, false, tid);

    // heads & $valid 作为新的有效位，仅保留“组首且命中”的代表行
    int vidx = v.colNum() + View::VALID_COL_OFFSET;
    auto hv = BoolAndBatchOperator(&heads, &v._dataCols[vidx], 1, 0, tid,
                                   SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    v._dataCols[vidx] = std::move(hv);
    return v;
}

// 用 NOT(rhs<lhs) 构造 (lhs <= rhs)
std::vector<int64_t> build_leq_bits(std::vector<int64_t> &lhs,
                                    std::vector<int64_t> &rhs,
                                    int baseTid) {
    auto rltl = BoolLessBatchOperator(&rhs, &lhs, 64, 0, baseTid,
                                      SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    if (Comm::rank() == 0) for (auto &b: rltl) b ^= 1; // NOT
    return rltl;
}

void find_time_cols(View &joined, int &left_time_idx, int &right_time_idx) {
    left_time_idx = right_time_idx = -1;
    for (int i = 0; i < (int) joined._fieldNames.size(); ++i) {
        auto &nm = joined._fieldNames[i];
        if (nm == "diagnosis.time" || (nm.find("diagnosis.") == 0 && nm.find(".time") != std::string::npos)) {
            left_time_idx = i;
        } else if (nm == "medication.time" || (nm.find("medication.") == 0 && nm.find(".time") != std::string::npos)) {
            right_time_idx = i;
        }
    }
    // 兜底：按位置猜测（左表 time 在 2，右表 time 在 6）
    if (left_time_idx < 0) left_time_idx = std::min(2, (int) joined._dataCols.size() - 1);
    if (right_time_idx < 0) right_time_idx = std::min(6, (int) joined._dataCols.size() - 1);
}
