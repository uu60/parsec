#include "secret/Secrets.h"
#include "secret/item/BoolSecret.h"
#include "utils/Log.h"
#include "utils/Math.h"
#include "utils/System.h"
#include "comm/Comm.h"

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

namespace {

struct Config {
    int n = 256;
    int width = 32;
    int clientRank = 2;
    bool asc = true;
    int printSamples = 8;
    int maxMismatchPrint = 16;
};

Config loadConfig() {
    Config cfg;
    if (Conf::_userParams.count("n")) {
        cfg.n = std::stoi(Conf::_userParams["n"]);
    }
    if (Conf::_userParams.count("width")) {
        cfg.width = std::stoi(Conf::_userParams["width"]);
    }
    if (Conf::_userParams.count("asc")) {
        const std::string v = Conf::_userParams["asc"];
        cfg.asc = (v == "1" || v == "true" || v == "True" || v == "YES" || v == "yes");
    }
    if (Conf::_userParams.count("print")) {
        cfg.printSamples = std::stoi(Conf::_userParams["print"]);
    }
    return cfg;
}

}

int main(int argc, char **argv) {
    System::init(argc, argv);

    const Config cfg = loadConfig();

    const int task = System::nextTask();

    std::vector<int64_t> plain;
    std::vector<int64_t> share0;
    std::vector<int64_t> share1;

    if (Comm::rank() == cfg.clientRank) {
        plain.resize(cfg.n);
        share0.resize(cfg.n);
        share1.resize(cfg.n);

        for (int i = 0; i < cfg.n; ++i) {
            plain[i] = Math::ring(Math::randInt(), cfg.width);
            share0[i] = Math::ring(Math::randInt(), cfg.width);
            share1[i] = Math::ring(share0[i] ^ plain[i], cfg.width);
        }

        const int tagS0 = task * 1000 + 11;
        const int tagS1 = task * 1000 + 12;
        Comm::send(share0, cfg.width, 0, tagS0);
        Comm::send(share1, cfg.width, 1, tagS1);
    }

    std::vector<int64_t> localShare;
    if (Comm::isServer()) {
        localShare.resize(cfg.n);
        const int tag = task * 1000 + (Comm::rank() == 0 ? 11 : 12);
        Comm::receive(localShare, cfg.width, cfg.clientRank, tag);

        std::vector<BoolSecret> secrets;
        secrets.reserve(cfg.n);
        for (int i = 0; i < cfg.n; ++i) {
            secrets.emplace_back(localShare[i], cfg.width, task, 0);
        }

        Secrets::sort(secrets, cfg.asc, task);

        std::vector<int64_t> sortedShare;
        sortedShare.reserve(cfg.n);
        for (int i = 0; i < cfg.n; ++i) {
            sortedShare.push_back(Math::ring(secrets[i]._data, cfg.width));
        }

        const int tagBack = task * 1000 + (Comm::rank() == 0 ? 21 : 22);
        Comm::send(sortedShare, cfg.width, cfg.clientRank, tagBack);
    }

    int64_t mismatchCount = 0;
    if (Comm::rank() == cfg.clientRank) {
        std::vector<int64_t> sorted0(cfg.n), sorted1(cfg.n);
        const int tagBack0 = task * 1000 + 21;
        const int tagBack1 = task * 1000 + 22;
        Comm::receive(sorted0, cfg.width, 0, tagBack0);
        Comm::receive(sorted1, cfg.width, 1, tagBack1);

        std::vector<int64_t> recon(cfg.n);
        for (int i = 0; i < cfg.n; ++i) {
            recon[i] = Math::ring(sorted0[i] ^ sorted1[i], cfg.width);
        }

        std::vector<int64_t> expected = plain;
        std::sort(expected.begin(), expected.end(), [&](int64_t a, int64_t b) {
            if (cfg.asc) return a < b;
            return a > b;
        });

        Log::i("[OT BoolShare sort correctness] n={}, width={}, asc={}", cfg.n, cfg.width, cfg.asc);

        const int samples = std::min(cfg.printSamples, cfg.n);
        for (int i = 0; i < samples; ++i) {
            Log::i("sample i={} exp={} got={}", i, expected[i], recon[i]);
        }

        int printed = 0;
        for (int i = 0; i < cfg.n; ++i) {
            if (recon[i] != expected[i]) {
                ++mismatchCount;
                if (printed < cfg.maxMismatchPrint) {
                    Log::e("MISMATCH i={} exp={} got={}", i, expected[i], recon[i]);
                    ++printed;
                }
            }
        }

        Log::i(mismatchCount == 0 ? "[OT BoolShare sort correctness] PASS"
                                 : "[OT BoolShare sort correctness] FAIL (mismatches={})",
               mismatchCount);
    }

    System::finalize();
    return (Comm::rank() == cfg.clientRank && mismatchCount != 0) ? 2 : 0;
}
