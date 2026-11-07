#include "parsec/comm/Comm.h"
#include "parsec/compute/batch/bool/BoolAndBatchOperator.h"
#include "parsec/compute/batch/bool/BoolEqualBatchOperator.h"
#include "parsec/compute/batch/bool/BoolLessBatchOperator.h"
#include "parsec/conf/AppConfig.h"
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
    int num = 10000;
    int width = 64;
    int rounds = 5;
    int warmup = 1;
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

void prepareInputs(int num, int width, std::vector<int64_t>& plainX, std::vector<int64_t>& plainY) {
    if (Comm::isClient()) {
        plainX.resize(num);
        plainY.resize(num);
        int64_t mask = width >= 63 ? -1LL : ((1LL << width) - 1);
        for (int i = 0; i < num; ++i) {
            plainX[i] = Math::randInt() & mask;
            plainY[i] = Math::randInt() & mask;
            if (i % 7 == 0) {
                plainY[i] = plainX[i];
            }
        }
    }
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

void printResult(const std::string& name, const BenchConfig& cfg, const std::vector<int64_t>& times) {
    if (!Comm::isServer() || Comm::rank() != 0) {
        return;
    }
    std::vector<int64_t> sorted = times;
    std::sort(sorted.begin(), sorted.end());
    auto min = sorted.empty() ? 0 : sorted.front();
    auto max = sorted.empty() ? 0 : sorted.back();
    auto median = sorted.empty() ? 0 : sorted[sorted.size() / 2];
    Log::i("EQUAL_COMPARE impl={} num={} width={} rounds={} avg_ms={} median_ms={} min_ms={} max_ms={}", name, cfg.num,
           cfg.width, cfg.rounds, average(times), median, min, max);
}
} // namespace

int main(int argc, char* argv[]) {
    System::init(argc, argv);

    BenchConfig cfg;
    cfg.num = AppConfig::getInt("num", cfg.num);
    cfg.width = AppConfig::getInt("width", cfg.width);
    cfg.rounds = AppConfig::getInt("rounds", cfg.rounds);
    cfg.warmup = AppConfig::getInt("warmup", cfg.warmup);

    if (cfg.width < 1 || cfg.width > 64) {
        throw std::runtime_error("width must be in [1, 64].");
    }

    std::vector<int64_t> plainX;
    std::vector<int64_t> plainY;
    prepareInputs(cfg.num, cfg.width, plainX, plainY);

    auto secretX = Secrets::boolShare(plainX, 2, cfg.width, 0);
    auto secretY = Secrets::boolShare(plainY, 2, cfg.width, 0);

    std::vector<int64_t> oldTimes;
    std::vector<int64_t> newTimes;
    oldTimes.reserve(cfg.rounds);
    newTimes.reserve(cfg.rounds);

    for (int i = 0; i < cfg.warmup + cfg.rounds; ++i) {
        if (Comm::isServer()) {
            auto task = System::nextTask();
            auto start = nowMs();
            auto result = oldEqual(&secretX, &secretY, cfg.width, task, 0);
            auto elapsed = nowMs() - start;
            if (i >= cfg.warmup) {
                oldTimes.push_back(elapsed);
            }
            if (Comm::rank() == 0 && i == 0) {
                Log::i("old_equal_rank0_share_checksum={}", checksum(result));
            }
        }
    }

    for (int i = 0; i < cfg.warmup + cfg.rounds; ++i) {
        if (Comm::isServer()) {
            auto task = System::nextTask();
            auto start = nowMs();
            auto result = newEqual(&secretX, &secretY, cfg.width, task, 0);
            auto elapsed = nowMs() - start;
            if (i >= cfg.warmup) {
                newTimes.push_back(elapsed);
            }
            if (Comm::rank() == 0 && i == 0) {
                Log::i("new_equal_rank0_share_checksum={}", checksum(result));
            }
        }
    }

    printResult("old_less_based", cfg, oldTimes);
    printResult("new_xor_tree_and", cfg, newTimes);

    if (Comm::isServer() && Comm::rank() == 0) {
        Log::i("EQUAL_COMPARE tag_stride_old={} tag_stride_new={} bmt_count_old={} bmt_count_new={}",
               oldEqualTagStride(), BoolEqualBatchOperator::tagStride(),
               2 * BoolLessBatchOperator::bmtCount(cfg.num, cfg.width) + BoolAndBatchOperator::bmtCount(cfg.num, 1),
               BoolEqualBatchOperator::bmtCount(cfg.num, cfg.width));
    }

    System::finalize();
    return 0;
}
