#include "ot/RandOtBatchOperator.h"
#include "ot/IknpOtBatchOperator.h"
#include "intermediate/IntermediateDataSupport.h"
#include "comm/Comm.h"
#include "conf/Conf.h"
#include "utils/Log.h"
#include "utils/Math.h"
#include "utils/System.h"

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

namespace {

struct Config {
    int n = 1 << 16;   // number of OTs (for IKNP). RandOT uses packed mode with nPacked = ceil(n/64).
    int width = 64;    // message width for IKNP; RandOT packed is always 64-bit limbs.
    int rounds = 5;
    int warmup = 1;
    int sender = 0;
    int taskBase = 1;
};

Config loadConfig() {
    Config cfg;
    if (Conf::_userParams.count("n")) cfg.n = std::stoi(Conf::_userParams["n"]);
    if (Conf::_userParams.count("width")) cfg.width = std::stoi(Conf::_userParams["width"]);
    if (Conf::_userParams.count("rounds")) cfg.rounds = std::stoi(Conf::_userParams["rounds"]);
    if (Conf::_userParams.count("warmup")) cfg.warmup = std::stoi(Conf::_userParams["warmup"]);
    if (Conf::_userParams.count("sender")) cfg.sender = std::stoi(Conf::_userParams["sender"]);
    return cfg;
}

struct Stats {
    double avgMs = 0;
    double minMs = 0;
    double maxMs = 0;
};

Stats statsOf(const std::vector<double> &xs) {
    Stats s;
    if (xs.empty()) return s;
    s.minMs = s.maxMs = xs[0];
    double sum = 0;
    for (double v : xs) {
        sum += v;
        s.minMs = std::min(s.minMs, v);
        s.maxMs = std::max(s.maxMs, v);
    }
    s.avgMs = sum / static_cast<double>(xs.size());
    return s;
}

static void fillVec64(std::vector<int64_t> &v, int n) {
    v.resize(n);
    for (int i = 0; i < n; ++i) {
        v[i] = Math::randInt();
    }
}

static void fillChoices01(std::vector<int> &v, int n) {
    v.resize(n);
    for (int i = 0; i < n; ++i) {
        v[i] = static_cast<int>(Math::randInt(0, 1));
    }
}

} // namespace

int main(int argc, char **argv) {
    System::init(argc, argv);

    if (Comm::isClient()) {
        System::finalize();
        return 0;
    }

    // RandOT depends on ROT; ensure prepared.
    IntermediateDataSupport::init();

    const Config cfg = loadConfig();
    const bool isSender = (Comm::rank() == cfg.sender);

    // ---------------- RandOT (packed / bits-mode) ----------------
    // Follow the working pattern from correctness_rand_ot.cpp.
    // Each int64_t is a 64-bit packed OT (ss0/ss1 are 64-bit messages; choices is packed bitmask).
    const int nPacked = (cfg.n + 63) / 64;
    std::vector<int64_t> randSs0, randSs1, randChoicesPacked;
    if (isSender) {
        fillVec64(randSs0, nPacked);
        fillVec64(randSs1, nPacked);
    } else {
        fillVec64(randChoicesPacked, nPacked);
    }

    auto runRand = [&](int taskTag, int msgTagOffset) -> double {
        RandOtBatchOperator op(cfg.sender,
                               isSender ? &randSs0 : nullptr,
                               isSender ? &randSs1 : nullptr,
                               !isSender ? &randChoicesPacked : nullptr,
                               taskTag,
                               msgTagOffset);
        const int64_t t0 = System::currentTimeMillis();
        op.execute();
        const int64_t t1 = System::currentTimeMillis();
        return static_cast<double>(t1 - t0);
    };

    // ---------------- IKNP (scalar-batch) ----------------
    // IKNP in this repo is scalar-choice mode: receiver provides vector<int> choices.
    // To keep benchmark minimal, create synthetic scalar inputs of length cfg.n.
    std::vector<int64_t> iknpMs0, iknpMs1;
    std::vector<int> iknpChoices;
    if (isSender) {
        fillVec64(iknpMs0, cfg.n);
        fillVec64(iknpMs1, cfg.n);
    } else {
        fillChoices01(iknpChoices, cfg.n);
    }

    auto runIknp = [&](int taskTag, int msgTagOffset) -> double {
        IknpOtBatchOperator op(cfg.sender,
                               isSender ? &iknpMs0 : nullptr,
                               isSender ? &iknpMs1 : nullptr,
                               isSender ? nullptr : &iknpChoices,
                               cfg.width,
                               taskTag,
                               msgTagOffset);
        const int64_t t0 = System::currentTimeMillis();
        op.execute();
        const int64_t t1 = System::currentTimeMillis();
        return static_cast<double>(t1 - t0);
    };

    // Warmup with distinct tags
    for (int i = 0; i < cfg.warmup; ++i) {
        (void)runRand(cfg.taskBase + 1, 100000 + i * 1000);
        (void)runIknp(cfg.taskBase + 2, 200000 + i * 1000);
    }

    std::vector<double> randMs;
    std::vector<double> iknpMs;
    randMs.reserve(cfg.rounds);
    iknpMs.reserve(cfg.rounds);

    for (int i = 0; i < cfg.rounds; ++i) {
        randMs.push_back(runRand(cfg.taskBase + 10, 300000 + i * 10000));
        iknpMs.push_back(runIknp(cfg.taskBase + 20, 400000 + i * 10000));
    }

    if (Comm::rank() == 0) {
        const Stats r = statsOf(randMs);
        const Stats k = statsOf(iknpMs);

        const double randTotalOT = static_cast<double>(nPacked) * 64.0;
        const double iknpTotalOT = static_cast<double>(cfg.n);

        Log::i("[OT speed] rounds={}, warmup={}, sender={}", cfg.rounds, cfg.warmup, cfg.sender);

        Log::i("[OT speed] RandOT(bits) nPacked={} ({} OTs): avg={}ms min={}ms max={}ms ({}us/OT)",
               nPacked,
               static_cast<int64_t>(randTotalOT),
               r.avgMs,
               r.minMs,
               r.maxMs,
               (r.avgMs * 1000.0) / randTotalOT);

        Log::i("[OT speed] IKNP n={} ({} OTs): avg={}ms min={}ms max={}ms ({}us/OT)",
               cfg.n,
               static_cast<int64_t>(iknpTotalOT),
               k.avgMs,
               k.minMs,
               k.maxMs,
               (k.avgMs * 1000.0) / iknpTotalOT);

        if (k.avgMs > 0) {
            Log::i("[OT speed] (avg ms) Rand/Iknp = {}x", r.avgMs / k.avgMs);
        }
    }

    System::finalize();
    return 0;
}
