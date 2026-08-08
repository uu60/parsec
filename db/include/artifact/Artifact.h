#ifndef PARSEC_DB_ARTIFACT_H
#define PARSEC_DB_ARTIFACT_H

#include "comm/Comm.h"
#include "conf/Conf.h"
#include "conf/DbConf.h"
#include "intermediate/BmtBatchGenerator.h"
#include "intermediate/BmtGenerator.h"
#include "intermediate/BitwiseBmtBatchGenerator.h"
#include "intermediate/BitwiseBmtGenerator.h"
#include "accelerate/SimdSupport.h"

#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <limits>
#include <random>
#include <stdexcept>
#include <string>

namespace Artifact {

inline uint64_t workloadSeed() {
    constexpr uint64_t defaultSeed = 20270276ULL;
    auto it = Conf::_userParams.find("workload_seed");
    if (it == Conf::_userParams.end()) {
        return defaultSeed;
    }
    try {
        size_t consumed = 0;
        const auto seed = std::stoull(it->second, &consumed, 0);
        if (consumed != it->second.size()) {
            throw std::invalid_argument("trailing characters");
        }
        return seed;
    } catch (const std::exception &) {
        throw std::runtime_error("--workload_seed must be an unsigned integer");
    }
}

inline std::mt19937_64 &workloadGenerator() {
    // This generator is exclusively for public benchmark input generation.
    // Protocol masks, OT, BMTs, and secret sharing continue to use Math's
    // cryptographically independent randomness.
    static std::mt19937_64 generator(workloadSeed());
    return generator;
}

inline int64_t workloadRandInt() {
    return static_cast<int64_t>(workloadGenerator()());
}

inline int64_t workloadRandInt(int64_t low, int64_t high) {
    if (low > high) {
        throw std::invalid_argument("workloadRandInt: low is greater than high");
    }
    std::uniform_int_distribution<int64_t> distribution(low, high);
    return distribution(workloadGenerator());
}

inline std::string configuration() {
    if (DbConf::BASELINE_MODE) {
        return "parsec_base";
    }
    if (DbConf::NO_COMPACTION) {
        return "parsec_noncompact";
    }
    return "parsec";
}

class Timer {
public:
    explicit Timer(std::string workload) : _workload(std::move(workload)) {
        Comm::barrier();
        _bmtStartedMillis = bmtMillis();
        _started = Clock::now();
    }

    Timer(const Timer &) = delete;
    Timer &operator=(const Timer &) = delete;

    void finish(int64_t outputRows = -1) {
        if (_finished) {
            return;
        }
        Comm::barrier();
        const auto ended = Clock::now();
        const double seconds = std::chrono::duration<double>(ended - _started).count();
        const auto bmtEndedMillis = bmtMillis();
        const double bmtSeconds = (_bmtStartedMillis < 0 || bmtEndedMillis < 0)
                                  ? -1.0
                                  : static_cast<double>(bmtEndedMillis - _bmtStartedMillis) / 1000.0;

        // One line per rank lets the artifact runner select max(server 0, 1)
        // while retaining rank-level diagnostics.  The prefix makes parsing
        // robust in the presence of ordinary timestamped log messages.
        std::cout << "ARTIFACT_METRIC {"
                  << "\"schema_version\":1,"
                  << "\"workload\":\"" << _workload << "\","
                  << "\"configuration\":\"" << configuration() << "\","
                  << "\"rank\":" << Comm::rank() << ','
                  << "\"elapsed_seconds\":" << std::fixed << std::setprecision(9) << seconds << ','
                  << "\"bmt_generator_accumulated_seconds\":" << bmtSeconds << ','
                  << "\"output_rows\":" << outputRows << ','
                  << "\"workload_seed\":" << workloadSeed() << ','
                  << "\"simd_enabled\":" << (Conf::ENABLE_SIMD ? "true" : "false") << ','
                  << "\"simd_backend\":\"" << SimdSupport::backend() << "\""
                  << "}" << std::endl;
        _finished = true;
    }

private:
    using Clock = std::chrono::steady_clock;
    std::string _workload;
    Clock::time_point _started{};
    int64_t _bmtStartedMillis{0};
    bool _finished{false};

    static int64_t bmtMillis() {
        if (!Conf::ENABLE_CLASS_WISE_TIMING) {
            return -1;
        }
        return BmtGenerator::_totalTime.load()
               + BmtBatchGenerator::_totalTime.load()
               + BitwiseBmtGenerator::_totalTime.load()
               + BitwiseBmtBatchGenerator::_totalTime.load();
    }
};

} // namespace Artifact

#endif
