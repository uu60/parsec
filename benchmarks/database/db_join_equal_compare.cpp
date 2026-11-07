#include "parsec/compute/batch/bool/BoolAndBatchOperator.h"
#include "parsec/compute/batch/bool/BoolEqualBatchOperator.h"
#include "parsec/compute/batch/bool/BoolLessBatchOperator.h"
#include "parsec/conf/AppConfig.h"
#include "parsec/db/DbConf.h"
#include "parsec/secret/Secrets.h"
#include "parsec/utils/Log.h"
#include "parsec/utils/Math.h"
#include "parsec/utils/System.h"

#include <algorithm>
#include <cstdint>
#include <numeric>
#include <string>
#include <vector>

using namespace parsec;

namespace {
struct BenchConfig {
    int rows = 100;
    int tableNum = 2;
    int width = 64;
    int rounds = 5;
    int warmup = 1;
};

struct PairInput {
    std::vector<int64_t> left;
    std::vector<int64_t> right;
};

int64_t nowMs() { return System::currentTimeMillis(); }

int oldEqualTagStride() { return 2 * BoolLessBatchOperator::tagStride() + BoolAndBatchOperator::tagStride(); }

std::vector<int64_t> oldEqual(std::vector<int64_t>* xs, std::vector<int64_t>* ys, int width, int taskTag,
                              int msgTagOffset) {
    auto lessXy =
        BoolLessBatchOperator(xs, ys, width, taskTag, msgTagOffset, SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    auto lessYx = BoolLessBatchOperator(ys, xs, width, taskTag, msgTagOffset + BoolLessBatchOperator::tagStride(),
                                        SecureOperator::NO_CLIENT_COMPUTE)
                      .execute()
                      ->_zis;

    for (auto& v : lessXy) {
        v ^= Comm::rank();
    }
    for (auto& v : lessYx) {
        v ^= Comm::rank();
    }

    return BoolAndBatchOperator(&lessXy, &lessYx, 1, taskTag, msgTagOffset + 2 * BoolLessBatchOperator::tagStride(),
                                SecureOperator::NO_CLIENT_COMPUTE)
        .execute()
        ->_zis;
}

std::vector<int64_t> newEqual(std::vector<int64_t>* xs, std::vector<int64_t>* ys, int width, int taskTag,
                              int msgTagOffset) {
    return BoolEqualBatchOperator(xs, ys, width, taskTag, msgTagOffset, SecureOperator::NO_CLIENT_COMPUTE)
        .execute()
        ->_zis;
}

void preparePlainTables(const BenchConfig& cfg, std::vector<std::vector<int64_t>>& tables) {
    tables.resize(cfg.tableNum);
    for (int tableIdx = 0; tableIdx < cfg.tableNum; ++tableIdx) {
        tables[tableIdx].resize(cfg.rows);
        if (!Comm::isClient()) {
            continue;
        }

        int64_t mask = cfg.width >= 63 ? -1LL : ((1LL << cfg.width) - 1);
        for (int row = 0; row < cfg.rows; ++row) {
            tables[tableIdx][row] = Math::randInt() & mask;
            if (tableIdx > 0 && row % 7 == 0) {
                tables[tableIdx][row] = tables[0][row];
            }
        }
    }
}

std::vector<PairInput> buildPairInputs(const std::vector<std::vector<int64_t>>& shares, const BenchConfig& cfg) {
    std::vector<PairInput> inputs;
    if (!Comm::isServer()) {
        return inputs;
    }

    inputs.reserve(cfg.tableNum - 1);
    for (int tableIdx = 1; tableIdx < cfg.tableNum; ++tableIdx) {
        PairInput input;
        const size_t pairCount = static_cast<size_t>(cfg.rows) * static_cast<size_t>(cfg.rows);
        input.left.reserve(pairCount);
        input.right.reserve(pairCount);
        for (int i = 0; i < cfg.rows; ++i) {
            for (int j = 0; j < cfg.rows; ++j) {
                input.left.push_back(shares[0][i]);
                input.right.push_back(shares[tableIdx][j]);
            }
        }
        inputs.push_back(std::move(input));
    }
    return inputs;
}

int64_t checksum(const std::vector<int64_t>& values) {
    return std::accumulate(values.begin(), values.end(), int64_t{0}, [](int64_t acc, int64_t v) { return acc ^ v; });
}

double average(const std::vector<int64_t>& values) {
    if (values.empty()) {
        return 0.0;
    }
    auto sum = std::accumulate(values.begin(), values.end(), int64_t{0});
    return static_cast<double>(sum) / static_cast<double>(values.size());
}

void printResult(const std::string& name, const BenchConfig& cfg, size_t pairCount, const std::vector<int64_t>& times) {
    if (!Comm::isServer() || Comm::rank() != 0) {
        return;
    }
    auto sorted = times;
    std::sort(sorted.begin(), sorted.end());
    auto median = sorted.empty() ? 0 : sorted[sorted.size() / 2];
    auto min = sorted.empty() ? 0 : sorted.front();
    auto max = sorted.empty() ? 0 : sorted.back();
    Log::i("DB_JOIN_EQUAL_COMPARE impl={} rows={} table_num={} width={} pair_comparisons={} rounds={} avg_ms={} "
           "median_ms={} min_ms={} max_ms={}",
           name, cfg.rows, cfg.tableNum, cfg.width, pairCount, cfg.rounds, average(times), median, min, max);
}
} // namespace

int main(int argc, char* argv[]) {
    System::init(argc, argv);
    DbConf::init();

    BenchConfig cfg;
    cfg.rows = AppConfig::getInt("rows", cfg.rows);
    cfg.tableNum = AppConfig::getInt("table_num", cfg.tableNum);
    cfg.width = AppConfig::getInt("width", cfg.width);
    cfg.rounds = AppConfig::getInt("rounds", cfg.rounds);
    cfg.warmup = AppConfig::getInt("warmup", cfg.warmup);

    if (cfg.rows <= 0) {
        throw std::runtime_error("rows must be positive.");
    }
    if (cfg.tableNum < 2) {
        throw std::runtime_error("table_num must be at least 2.");
    }
    if (cfg.width < 1 || cfg.width > 64) {
        throw std::runtime_error("width must be in [1, 64].");
    }

    std::vector<std::vector<int64_t>> tables;
    preparePlainTables(cfg, tables);

    std::vector<std::vector<int64_t>> shares(cfg.tableNum);
    for (int tableIdx = 0; tableIdx < cfg.tableNum; ++tableIdx) {
        shares[tableIdx] = Secrets::boolShare(tables[tableIdx], 2, cfg.width, 0);
    }

    auto pairInputs = buildPairInputs(shares, cfg);
    const size_t pairCount =
        static_cast<size_t>(cfg.tableNum - 1) * static_cast<size_t>(cfg.rows) * static_cast<size_t>(cfg.rows);

    std::vector<int64_t> oldTimes;
    std::vector<int64_t> newTimes;
    oldTimes.reserve(cfg.rounds);
    newTimes.reserve(cfg.rounds);

    for (int iter = 0; iter < cfg.warmup + cfg.rounds; ++iter) {
        if (Comm::isServer()) {
            const int task = System::nextTask();
            int tag = 0;
            int64_t combined = 0;
            auto start = nowMs();
            for (auto& input : pairInputs) {
                auto result = oldEqual(&input.left, &input.right, cfg.width, task, tag);
                combined ^= checksum(result);
                tag += oldEqualTagStride();
            }
            auto elapsed = nowMs() - start;
            if (iter >= cfg.warmup) {
                oldTimes.push_back(elapsed);
            }
            if (Comm::rank() == 0 && iter == 0) {
                Log::i("old_join_equal_rank0_share_checksum={}", combined);
            }
        }
    }

    for (int iter = 0; iter < cfg.warmup + cfg.rounds; ++iter) {
        if (Comm::isServer()) {
            const int task = System::nextTask();
            int tag = 0;
            int64_t combined = 0;
            auto start = nowMs();
            for (auto& input : pairInputs) {
                auto result = newEqual(&input.left, &input.right, cfg.width, task, tag);
                combined ^= checksum(result);
                tag += BoolEqualBatchOperator::tagStride();
            }
            auto elapsed = nowMs() - start;
            if (iter >= cfg.warmup) {
                newTimes.push_back(elapsed);
            }
            if (Comm::rank() == 0 && iter == 0) {
                Log::i("new_join_equal_rank0_share_checksum={}", combined);
            }
        }
    }

    printResult("old_less_based", cfg, pairCount, oldTimes);
    printResult("new_xor_tree_and", cfg, pairCount, newTimes);

    if (Comm::isServer() && Comm::rank() == 0) {
        const int batchCount = static_cast<int>(pairCount);
        Log::i("DB_JOIN_EQUAL_COMPARE tag_stride_old={} tag_stride_new={} bmt_count_old={} bmt_count_new={}",
               oldEqualTagStride(), BoolEqualBatchOperator::tagStride(),
               2 * BoolLessBatchOperator::bmtCount(batchCount, cfg.width) +
                   BoolAndBatchOperator::bmtCount(batchCount, 1),
               BoolEqualBatchOperator::bmtCount(batchCount, cfg.width));
    }

    System::finalize();
    return 0;
}
