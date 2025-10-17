
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

View perPidMinOrMax(View v, bool keepMinTime, int tid);

std::vector<int64_t> build_leq_bits(std::vector<int64_t> &lhs,
                                    std::vector<int64_t> &rhs,
                                    int baseTid);

void find_time_cols(View &joined, int &left_time_idx, int &right_time_idx);

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

    std::vector<int64_t> d_pid, d_diag, d_time, d_tag;
    std::vector<int64_t> m_pid, m_med, m_time, m_tag;
    generateTestData(diagRows, medRows, d_pid, d_diag, d_time, d_tag, m_pid, m_med, m_time, m_tag);

    auto d_pid_s = Secrets::boolShare(d_pid, 2, 64, tid);
    auto d_diag_s = Secrets::boolShare(d_diag, 2, 64, tid);
    auto d_time_s = Secrets::boolShare(d_time, 2, 64, tid);
    auto d_tag_s = Secrets::boolShare(d_tag, 2, 64, tid);

    auto m_pid_s = Secrets::boolShare(m_pid, 2, 64, tid);
    auto m_med_s = Secrets::boolShare(m_med, 2, 64, tid);
    auto m_time_s = Secrets::boolShare(m_time, 2, 64, tid);
    auto m_tag_s = Secrets::boolShare(m_tag, 2, 64, tid);

    int64_t hd_share, asp_share;
    if (Comm::isClient()) {
        int64_t r0 = Math::randInt();
        int64_t r1 = Math::randInt();
        Comm::send(r0, 64, 0, tid);
        Comm::send(r0 ^ kHD, 64, 1, tid);
        Comm::send(r1, 64, 0, tid);
        Comm::send(r1 ^ kASP, 64, 1, tid);
    } else {
        Comm::receive(hd_share, 64, 2, tid);
        Comm::receive(asp_share, 64, 2, tid);
    }

    if (Comm::isServer()) {
        auto diagnosis_view = createDiagnosisTable(d_pid_s, d_diag_s, d_time_s, d_tag_s);
        auto medication_view = createMedicationTable(m_pid_s, m_med_s, m_time_s, m_tag_s);

        std::string col_diag = "diag";
        std::string col_med = "med";

        View d_min, m_max;
        auto t0 = System::currentTimeMillis();
        if (DbConf::BASELINE_MODE) {
            auto dv = filterEquals(diagnosis_view, col_diag, hd_share, tid);
            auto mv = filterEquals(medication_view, col_med, asp_share, tid);

            d_min = perPidMinOrMax(dv, true, tid);
            m_max = perPidMinOrMax(mv, false, tid);
        } else {
            auto f = ThreadPoolSupport::submit([&] {
                auto dv = filterEquals(diagnosis_view, col_diag, hd_share, tid);
                auto d_min = perPidMinOrMax(dv, true, tid);
                return d_min;
            });
            auto mv = filterEquals(medication_view, col_med, asp_share, tid + 1000);
            m_max = perPidMinOrMax(mv, false, tid + 1000);

            d_min = f.get();
        }


        std::string field0 = "pid";
        auto joined = Views::nestedLoopJoin(d_min, m_max, field0, field0, false);

        int d_time_idx = -1, m_time_idx = -1;
        find_time_cols(joined, d_time_idx, m_time_idx);

        auto &d_time_col = joined._dataCols[d_time_idx];
        auto &m_time_col = joined._dataCols[m_time_idx];
        auto le_bits = build_leq_bits(d_time_col, m_time_col, tid);

        auto valids = BoolAndBatchOperator(&le_bits, &joined._dataCols[joined.colNum() + View::VALID_COL_OFFSET],
                                           1, 0, tid,
                                           SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        int valid_idx = joined.colNum() + View::VALID_COL_OFFSET;
        joined._dataCols[valid_idx] = std::move(valids);

        Log::i("Total query execution time: {}ms", System::currentTimeMillis() - t0);
    }

    System::finalize();
    return 0;
}

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

            diagnosis_tag_data.push_back(Views::hash(pid));
        }

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
        t.insert({pid[i], diag[i], time[i], tag[i]});
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

View perPidMinOrMax(View v, bool keepMinTime, int tid) {
    std::vector<std::string> order = {"pid", "$valid", "time"};
    std::vector<bool> asc = {true, false, keepMinTime ? true : false};
    v.sort(order, asc, tid);

    std::string grp_col = "pid";
    auto heads = v.groupBy(grp_col, false, tid);

    int vidx = v.colNum() + View::VALID_COL_OFFSET;
    auto hv = BoolAndBatchOperator(&heads, &v._dataCols[vidx], 1, 0, tid,
                                   SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    v._dataCols[vidx] = std::move(hv);
    return v;
}

std::vector<int64_t> build_leq_bits(std::vector<int64_t> &lhs,
                                    std::vector<int64_t> &rhs,
                                    int baseTid) {
    auto rltl = BoolLessBatchOperator(&rhs, &lhs, 64, 0, baseTid,
                                      SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    if (Comm::rank() == 0) for (auto &b: rltl) b ^= 1;
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
    if (left_time_idx < 0) left_time_idx = std::min(2, (int) joined._dataCols.size() - 1);
    if (right_time_idx < 0) right_time_idx = std::min(6, (int) joined._dataCols.size() - 1);
}
