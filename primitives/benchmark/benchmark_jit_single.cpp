//
// Created by 杜建璋 on 25-7-28.
// Single test for JIT mode - to be called by Python script
//

#include "compute/batch/bool/BoolLessBatchOperator.h"
#include "compute/single/arith/ArithLessOperator.h"
#include "secret/Secrets.h"
#include "utils/Log.h"
#include "utils/Math.h"
#include "utils/System.h"
#include "comm/Comm.h"
#include <fstream>
#include <iomanip>
#include <sstream>
#include <chrono>
#include <numeric>
#include <map>

#include "compute/batch/bool/BoolEqualBatchOperator.h"
#include "compute/batch/bool/BoolMutexBatchOperator.h"
#include "parallel/ThreadPoolSupport.h"

#include <string>

static int task;

void prepareOrigins(int num, std::vector<int64_t> &originsA, std::vector<int64_t> &originsB,
                    std::vector<int64_t> &conditions) {
    if (Comm::isClient()) {
        originsA.resize(num);
        originsB.resize(num);
        conditions.resize(num);
        for (int i = 0; i < num; i++) {
            originsA[i] = Math::randInt(0, num);
            originsB[i] = Math::randInt(0, num);
            conditions[i] = Math::randInt(0, 1);
        }
    }
    task = System::nextTask();
}

void boolShare(int width, std::vector<int64_t> &originsA, std::vector<int64_t> &originsB,
               std::vector<int64_t> &conditions, std::vector<int64_t> &secretsA,
               std::vector<int64_t> &secretsB, std::vector<int64_t> &secretConditions) {
    secretsA = Secrets::boolShare(originsA, 2, width, 0);
    secretsB = Secrets::boolShare(originsB, 2, width, 0);
    secretConditions = Secrets::boolShare(conditions, 2, 1, 0);
}

bool testJit(std::string pmt, int width, std::vector<int64_t> &originsA,
             std::vector<int64_t> &originsB, std::vector<int64_t> &conditions, int64_t &jitTime) {
    std::vector<int64_t> secretsA;
    std::vector<int64_t> secretsB;
    std::vector<int64_t> secretConditions;

    boolShare(width, originsA, originsB, conditions, secretsA, secretsB, secretConditions);

    int batch_size = Conf::BATCH_SIZE;
    int batch_num = (static_cast<int>(secretsA.size()) + batch_size - 1) / batch_size;
    if (Comm::isServer()) {
        int64_t start = System::currentTimeMillis();
        if (pmt == "<") {
            // "<"
            std::vector<int64_t> zs;
            zs.reserve(secretsA.size());
            std::vector<std::future<std::vector<int64_t> > > futures(batch_num);
            for (int b = 0; b < batch_num; ++b) {
                futures[b] = ThreadPoolSupport::submit([&, b] {
                    int startIdx = b * batch_size;
                    int endIdx = std::min((b + 1) * batch_size, static_cast<int>(secretsA.size()));
                    std::vector<int64_t> batchA(secretsA.begin() + startIdx, secretsA.begin() + endIdx);
                    std::vector<int64_t> batchB(secretsB.begin() + startIdx, secretsB.begin() + endIdx);
                    return BoolLessBatchOperator(&batchA, &batchB, width, 0, BoolLessBatchOperator::tagStride() * b,
                                                 -1).
                            execute()->_zis;
                });
            }
            for (auto &f: futures) {
                auto v = f.get();
                zs.insert(zs.end(), v.begin(), v.end());
            }
        } else if (pmt == "<=") {
            // "<="
            std::vector<int64_t> zs;
            zs.reserve(secretsA.size());
            std::vector<std::future<std::vector<int64_t> > > futures(batch_num);
            for (int b = 0; b < batch_num; ++b) {
                futures[b] = ThreadPoolSupport::submit([&, b] {
                    int startIdx = b * batch_size;
                    int endIdx = std::min((b + 1) * batch_size, static_cast<int>(secretsA.size()));
                    std::vector<int64_t> batchA(secretsA.begin() + startIdx, secretsA.begin() + endIdx);
                    std::vector<int64_t> batchB(secretsB.begin() + startIdx, secretsB.begin() + endIdx);
                    auto zsB = BoolLessBatchOperator(&batchB, &batchA, width, 0,
                                                     BoolLessBatchOperator::tagStride() * b,
                                                     -1).
                            execute()->_zis;
                    for (auto &t: zsB) {
                        t = t ^ Comm::rank();
                    }
                    return zsB;
                });
            }
            for (auto &f: futures) {
                auto v = f.get();
                zs.insert(zs.end(), v.begin(), v.end());
            }
        } else if (pmt == "==") {
            // "=="
            std::vector<int64_t> zs;
            zs.reserve(secretsA.size());
            std::vector<std::future<std::vector<int64_t> > > futures(batch_num);
            for (int b = 0; b < batch_num; ++b) {
                futures[b] = ThreadPoolSupport::submit([&, b] {
                    int startIdx = b * batch_size;
                    int endIdx = std::min((b + 1) * batch_size, static_cast<int>(secretsA.size()));
                    std::vector<int64_t> batchA(secretsA.begin() + startIdx, secretsA.begin() + endIdx);
                    std::vector<int64_t> batchB(secretsB.begin() + startIdx, secretsB.begin() + endIdx);
                    return BoolEqualBatchOperator(&batchA, &batchB, width, 0,
                                                  BoolEqualBatchOperator::tagStride() * b,
                                                  -1).execute()->_zis;
                });
            }
            for (auto &f: futures) {
                auto v = f.get();
                zs.insert(zs.end(), v.begin(), v.end());
            }
        } else if (pmt == "!=") {
            // "!="
            std::vector<int64_t> zs;
            zs.reserve(secretsA.size());
            std::vector<std::future<std::vector<int64_t> > > futures(batch_num);
            for (int b = 0; b < batch_num; ++b) {
                futures[b] = ThreadPoolSupport::submit([&, b] {
                    int startIdx = b * batch_size;
                    int endIdx = std::min((b + 1) * batch_size, static_cast<int>(secretsA.size()));
                    std::vector<int64_t> batchA(secretsA.begin() + startIdx, secretsA.begin() + endIdx);
                    std::vector<int64_t> batchB(secretsB.begin() + startIdx, secretsB.begin() + endIdx);
                    auto zsB = BoolEqualBatchOperator(&batchA, &batchB, width, 0,
                                                      BoolEqualBatchOperator::tagStride() * b,
                                                      -1).
                            execute()->_zis;
                    for (auto &t: zsB) {
                        t = t ^ Comm::rank();
                    }
                    return zsB;
                });
            }
            for (auto &f: futures) {
                auto v = f.get();
                zs.insert(zs.end(), v.begin(), v.end());
            }
        } else if (pmt == "mux") {
            // "mux"
            std::vector<int64_t> zs;
            zs.reserve(secretsA.size());
            std::vector<std::future<std::vector<int64_t> > > futures(batch_num);
            for (int b = 0; b < batch_num; ++b) {
                futures[b] = ThreadPoolSupport::submit([&, b] {
                    int startIdx = b * batch_size;
                    int endIdx = std::min((b + 1) * batch_size, static_cast<int>(secretsA.size()));
                    std::vector<int64_t> batchA(secretsA.begin() + startIdx, secretsA.begin() + endIdx);
                    std::vector<int64_t> batchB(secretsB.begin() + startIdx, secretsB.begin() + endIdx);
                    std::vector<int64_t> batchC(secretConditions.begin() + startIdx,
                                                secretConditions.begin() + endIdx);
                    return BoolMutexBatchOperator(&batchA, &batchB, &batchC, width, 0,
                                                  BoolMutexBatchOperator::tagStride() * b,
                                                  -1).
                            execute()->_zis;
                });
            }
            for (auto &f: futures) {
                auto v = f.get();
                zs.insert(zs.end(), v.begin(), v.end());
            }
        } else if (pmt == "sort") {
            // "sort"
            std::vector<BoolSecret> boolSecrets;
            boolSecrets.reserve(secretsA.size());
            for (size_t i = 0; i < secretsA.size(); i++) {
                boolSecrets.emplace_back(secretsA[i], width, 0, 0);
            }
            start = System::currentTimeMillis();
            Secrets::sort(boolSecrets, true, 0);
        } else {
            Log::e("Unknown primitive: {}", pmt);
            return false;
        }
        jitTime = System::currentTimeMillis() - start;
    }
    return true;
}

int main(int argc, char *argv[]) {
    System::init(argc, argv);

    // Read parameters from command line
    std::string primitive = "==";  // default
    int num = 1000;               // default
    int width = 64;               // default

    if (Conf::_userParams.count("primitive")) {
        primitive = Conf::_userParams["primitive"];
    }
    if (Conf::_userParams.count("num")) {
        num = std::stoi(Conf::_userParams["num"]);
    }
    if (Conf::_userParams.count("width")) {
        width = std::stoi(Conf::_userParams["width"]);
    }

    if (Comm::isClient()) {
        Log::i("JIT benchmark - Primitive: {}, Num: {}, Width: {}", primitive, num, width);
    }

    // Prepare test data
    std::vector<int64_t> originsA, originsB, conditions;
    prepareOrigins(num, originsA, originsB, conditions);

    // Run JIT test
    int64_t jitTime;
    if (!testJit(primitive, width, originsA, originsB, conditions, jitTime)) {
        if (Comm::isClient()) {
            Log::e("Test failed for primitive: {}", primitive);
        }
        System::finalize();
        return 1;
    }

    // Collect timing data from servers
    if (Comm::isServer()) {
        // Send timing data to client (rank 2)
        Comm::send(jitTime, 64, 2, 0);
    } else if (Comm::isClient()) {
        // Client receives timing data from both servers (rank 0 and 1)
        int64_t jitTime0, jitTime1;

        Comm::receive(jitTime0, 64, 0, 0);
        Comm::receive(jitTime1, 64, 1, 0);

        // Calculate average time
        double avgJitTime = (jitTime0 + jitTime1) / 2.0;

        // Output result in a format that Python can easily parse
        std::cout << "RESULT:" << primitive << "," << num << "," << width << "," 
                  << jitTime0 << "," << jitTime1 << "," << avgJitTime << std::endl;
        
        Log::i("JIT - Primitive: {}, Num: {}, Width: {} - Server0: {}ms, Server1: {}ms, Average: {}ms",
               primitive, num, width, jitTime0, jitTime1, avgJitTime);
    }

    System::finalize();
    return 0;
}
