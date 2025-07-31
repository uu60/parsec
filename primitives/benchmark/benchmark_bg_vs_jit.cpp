//
// Created by 杜建璋 on 25-7-28.
//

#include "compute/batch/arith/ArithLessBatchOperator.h"
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

#include "compute/batch/arith/ArithEqualBatchOperator.h"
#include "compute/batch/arith/ArithMutexBatchOperator.h"
#include "compute/batch/bool/BoolEqualBatchOperator.h"
#include "compute/batch/bool/BoolMutexBatchOperator.h"
#include "compute/batch/bool/BoolToArithBatchOperator.h"
#include "intermediate/IntermediateDataSupport.h"
#include "parallel/ThreadPoolSupport.h"

#include <string>
static int task = System::nextTask();

void prepareOrigins(int num, std::vector<int64_t> &originsA, std::vector<int64_t> &originsB,
                    std::vector<int64_t> &conditions) {
    // arith compare less than
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
}

void boolShare(int width, std::vector<int64_t> &originsA, std::vector<int64_t> &originsB,
               std::vector<int64_t> &conditions, std::vector<int64_t> &secretsA,
               std::vector<int64_t> &secretsB, std::vector<int64_t> &secretConditions) {
    secretsA = Secrets::boolShare(originsA, 2, width, 0);
    secretsB = Secrets::boolShare(originsB, 2, width, 0);
    secretConditions = Secrets::boolShare(conditions, 2, 1, 0);
}

bool testBackground(std::vector<std::string> &testPmts, std::string pmt, int width, std::vector<int64_t> &originsA,
                    std::vector<int64_t> &originsB, std::vector<int64_t> &conditions, int64_t &backgroundTime) {

    if (Conf::BMT_METHOD != Conf::BMT_BACKGROUND) {
        Conf::BMT_METHOD = Conf::BMT_BACKGROUND;
        IntermediateDataSupport::init();
    }

    std::vector<int64_t> secretsA;
    std::vector<int64_t> secretsB;
    std::vector<int64_t> secretConditions;

    boolShare(width, originsA, originsB, conditions, secretsA, secretsB, secretConditions);

    int batch_size = Conf::BATCH_SIZE;
    int batch_num = (static_cast<int>(secretsA.size()) + batch_size - 1) / batch_size;
    if (Comm::isServer()) {
        int64_t start = System::currentTimeMillis();
        if (pmt == testPmts[0]) {
            // "<"
            std::vector<int64_t> zs;
            zs.reserve(secretsA.size());
            std::vector<std::future<std::vector<int64_t> > > futures(batch_num);
            for (int b = 0; b < batch_num; ++b) {
                if (Conf::BMT_METHOD == Conf::BMT_BACKGROUND) {
                    int startIdx = b * batch_size;
                    int endIdx = std::min((b + 1) * batch_size, static_cast<int>(secretsA.size()));
                    std::shared_ptr<std::vector<BitwiseBmt> > bmtB = std::make_shared<std::vector<BitwiseBmt> >(
                        IntermediateDataSupport::pollBitwiseBmts(
                            BoolLessBatchOperator::bmtCount(endIdx - startIdx, width), 64));
                    futures[b] = ThreadPoolSupport::submit([&, b, bmtB, startIdx, endIdx] {
                        std::vector<int64_t> batchA(secretsA.begin() + startIdx, secretsA.begin() + endIdx);
                        std::vector<int64_t> batchB(secretsB.begin() + startIdx, secretsB.begin() + endIdx);
                        return BoolLessBatchOperator(&batchA, &batchB, width, task, BoolLessBatchOperator::tagStride() * b,
                                                     -1).setBmts(bmtB.get())->execute()->_zis;
                    });
                }
            }
            for (auto &f: futures) {
                auto v = f.get();
                zs.insert(zs.end(), v.begin(), v.end());
            }
        } else if (pmt == testPmts[1]) {
            // "<="
            std::vector<int64_t> zs;
            zs.reserve(secretsA.size());
            std::vector<std::future<std::vector<int64_t> > > futures(batch_num);
            for (int b = 0; b < batch_num; ++b) {
                if (Conf::BMT_METHOD == Conf::BMT_BACKGROUND) {
                    int startIdx = b * batch_size;
                    int endIdx = std::min((b + 1) * batch_size, static_cast<int>(secretsA.size()));
                    std::shared_ptr<std::vector<BitwiseBmt> > bmtB = std::make_shared<std::vector<BitwiseBmt> >(
                        IntermediateDataSupport::pollBitwiseBmts(
                            BoolLessBatchOperator::bmtCount(endIdx - startIdx, width), 64));
                    futures[b] = ThreadPoolSupport::submit([&, b, bmtB, startIdx, endIdx] {
                        std::vector<int64_t> batchA(secretsA.begin() + startIdx, secretsA.begin() + endIdx);
                        std::vector<int64_t> batchB(secretsB.begin() + startIdx, secretsB.begin() + endIdx);
                        auto zsB = BoolLessBatchOperator(&batchB, &batchA, width, task,
                                                         BoolLessBatchOperator::tagStride() * b,
                                                         -1).setBmts(bmtB.get())->execute()->_zis;
                        for (auto &t: zsB) {
                            t = t ^ Comm::rank();
                        }
                        return zsB;
                    });
                }
            }
            for (auto &f: futures) {
                auto v = f.get();
                zs.insert(zs.end(), v.begin(), v.end());
            }
        } else if (pmt == testPmts[2]) {
            // "=="
            std::vector<int64_t> zs;
            zs.reserve(secretsA.size());
            std::vector<std::future<std::vector<int64_t> > > futures(batch_num);
            for (int b = 0; b < batch_num; ++b) {
                if (Conf::BMT_METHOD == Conf::BMT_BACKGROUND) {
                    int startIdx = b * batch_size;
                    int endIdx = std::min((b + 1) * batch_size, static_cast<int>(secretsA.size()));
                    std::shared_ptr<std::vector<BitwiseBmt> > bmtB = std::make_shared<std::vector<BitwiseBmt> >(
                        IntermediateDataSupport::pollBitwiseBmts(
                            BoolEqualBatchOperator::bmtCount(endIdx - startIdx, width), 64));
                    futures[b] = ThreadPoolSupport::submit([&, b, bmtB, startIdx, endIdx] {
                        std::vector<int64_t> batchA(secretsA.begin() + startIdx, secretsA.begin() + endIdx);
                        std::vector<int64_t> batchB(secretsB.begin() + startIdx, secretsB.begin() + endIdx);
                        return BoolEqualBatchOperator(&batchA, &batchB, width, task,
                                                      BoolEqualBatchOperator::tagStride() * b,
                                                      -1).setBmts(bmtB.get())->execute()->_zis;
                    });
                }
            }
            for (auto &f: futures) {
                auto v = f.get();
                zs.insert(zs.end(), v.begin(), v.end());
            }
        } else if (pmt == testPmts[3]) {
            // "!="
            std::vector<int64_t> zs;
            zs.reserve(secretsA.size());
            std::vector<std::future<std::vector<int64_t> > > futures(batch_num);
            for (int b = 0; b < batch_num; ++b) {
                if (Conf::BMT_METHOD == Conf::BMT_BACKGROUND) {
                    int startIdx = b * batch_size;
                    int endIdx = std::min((b + 1) * batch_size, static_cast<int>(secretsA.size()));
                    std::shared_ptr<std::vector<BitwiseBmt> > bmtB = std::make_shared<std::vector<BitwiseBmt> >(
                        IntermediateDataSupport::pollBitwiseBmts(
                            BoolEqualBatchOperator::bmtCount(endIdx - startIdx, width), 64));
                    futures[b] = ThreadPoolSupport::submit([&, b, bmtB, startIdx, endIdx] {
                        std::vector<int64_t> batchA(secretsA.begin() + startIdx, secretsA.begin() + endIdx);
                        std::vector<int64_t> batchB(secretsB.begin() + startIdx, secretsB.begin() + endIdx);
                        auto zsB = BoolEqualBatchOperator(&batchA, &batchB, width, task,
                                                          BoolEqualBatchOperator::tagStride() * b,
                                                          -1).setBmts(bmtB.get())->execute()->_zis;
                        for (auto &t: zsB) {
                            t = t ^ Comm::rank();
                        }
                        return zsB;
                    });
                }
            }
            for (auto &f: futures) {
                auto v = f.get();
                zs.insert(zs.end(), v.begin(), v.end());
            }
        } else if (pmt == testPmts[4]) {
            // "mux"
            std::vector<int64_t> zs;
            zs.reserve(secretsA.size());
            std::vector<std::future<std::vector<int64_t> > > futures(batch_num);
            for (int b = 0; b < batch_num; ++b) {
                if (Conf::BMT_METHOD == Conf::BMT_BACKGROUND) {
                    int startIdx = b * batch_size;
                    int endIdx = std::min((b + 1) * batch_size, static_cast<int>(secretsA.size()));
                    std::shared_ptr<std::vector<BitwiseBmt> > bmtB = std::make_shared<std::vector<BitwiseBmt> >(
                        IntermediateDataSupport::pollBitwiseBmts(
                            BoolMutexBatchOperator::bmtCount(endIdx - startIdx, width), 64));
                    futures[b] = ThreadPoolSupport::submit([&, b, bmtB, startIdx, endIdx] {
                        std::vector<int64_t> batchA(secretsA.begin() + startIdx, secretsA.begin() + endIdx);
                        std::vector<int64_t> batchB(secretsB.begin() + startIdx, secretsB.begin() + endIdx);
                        std::vector<int64_t> batchC(secretConditions.begin() + startIdx,
                                                    secretConditions.begin() + endIdx);
                        return BoolMutexBatchOperator(&batchA, &batchB, &batchC, width, task,
                                                      BoolMutexBatchOperator::tagStride() * b,
                                                      -1).setBmts(bmtB.get())->execute()->_zis;
                    });
                }
            }
            for (auto &f: futures) {
                auto v = f.get();
                zs.insert(zs.end(), v.begin(), v.end());
            }
        } else if (pmt == testPmts[5]) {
            // "sort"
            std::vector<BoolSecret> boolSecrets;
            boolSecrets.reserve(secretsA.size());
            for (size_t i = 0; i < secretsA.size(); i++) {
                boolSecrets.emplace_back(secretsA[i], width, task, 0);
            }
            start = System::currentTimeMillis();
            Secrets::sort(boolSecrets, true, task);
        } else {
            Log::e("Unknown primitive: {}", pmt);
            return false;
        }
        backgroundTime = System::currentTimeMillis() - start;
    }
    return true;
}

bool testJit(std::vector<std::string> &testPmts, std::string pmt, int width, std::vector<int64_t> &originsA,
             std::vector<int64_t> &originsB, std::vector<int64_t> &conditions, int64_t &jitTime) {
    std::vector<int64_t> secretsA;
    std::vector<int64_t> secretsB;
    std::vector<int64_t> secretConditions;

    boolShare(width, originsA, originsB, conditions, secretsA, secretsB, secretConditions);

    int batch_size = Conf::BATCH_SIZE;
    int batch_num = (static_cast<int>(secretsA.size()) + batch_size - 1) / batch_size;
    if (Comm::isServer()) {
        int64_t start = System::currentTimeMillis();
        if (pmt == testPmts[0]) {
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
        } else if (pmt == testPmts[1]) {
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
        } else if (pmt == testPmts[2]) {
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
        } else if (pmt == testPmts[3]) {
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
        } else if (pmt == testPmts[4]) {
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
        } else if (pmt == testPmts[5]) {
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

    // CSV data collection structure
    struct BenchmarkResult {
        std::string primitive;
        int num{};
        int width{};
        double avgBackgroundTime{};
        double avgJitTime{};
        std::string timestamp;
    };

    std::vector<BenchmarkResult> results;

    // Generate timestamp for filename
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    std::stringstream ss;
    ss << std::put_time(std::localtime(&time_t), "%Y%m%d_%H%M%S");
    std::string timestamp = ss.str();

    std::vector testNums = {10000, /*100000, 1000000*//*, 10000000*/};
    std::vector testWidths = {1, 2, 4, 8, 16, 32, 64,};
    std::vector<std::string> testPmts = {"<", "<=", "==", "!=", "mux", "sort"};

    // Calculate total number of tests for progress tracking
    int totalTests = testPmts.size() * testNums.size() * testWidths.size() * 2; // *2 for both JIT and Background
    int currentTest = 0;

    // Store timing results for pairing JIT and Background results
    std::map<std::string, int64_t> jitResults; // key: "pmt_num_width"
    std::map<std::string, int64_t> backgroundResults;

    // First, run all JIT tests
    if (Comm::isClient()) {
        Log::i("Starting JIT tests...");
    }
    for (std::string pmt: testPmts) {
        if (Comm::isClient()) {
            Log::i("Starting JIT tests for primitive: {}", pmt);
        }
        for (int num: testNums) {
            for (int width: testWidths) {
                currentTest++;
                if (Comm::isClient()) {
                    Log::i("Progress: {}/{} - Testing JIT primitive: {}, num: {}, width: {}",
                           currentTest, totalTests, pmt, num, width);
                }
                std::vector<int64_t> originsA, originsB, conditions;
                prepareOrigins(num, originsA, originsB, conditions);

                int64_t jitTime;
                if (!testJit(testPmts, pmt, width, originsA, originsB, conditions, jitTime))
                    continue;

                // Collect timing data from servers
                if (Comm::isServer()) {
                    // Send timing data to client (rank 2)
                    Comm::send(jitTime, 64, 2, 0);
                } else if (Comm::isClient()) {
                    // Client receives timing data from both servers (rank 0 and 1)
                    int64_t jitTime0, jitTime1;

                    Comm::receive(jitTime0, 64, 0, 0);
                    Comm::receive(jitTime1, 64, 1, 0);

                    // Calculate average time and store
                    double avgJitTime = (jitTime0 + jitTime1) / 2.0;
                    std::string key = pmt + "_" + std::to_string(num) + "_" + std::to_string(width);
                    jitResults[key] = static_cast<int64_t>(avgJitTime);

                    Log::i("JIT - Primitive: {}, Num: {}, Width: {} - Server0: {}ms, Server1: {}ms, Average: {}ms",
                           pmt, num, width, jitTime0, jitTime1, avgJitTime);
                }
            }
        }
    }

    // Then, run all Background tests
    if (Comm::isClient()) {
        Log::i("Starting Background tests...");
    }
    for (const std::string& pmt: testPmts) {
        if (Comm::isClient()) {
            Log::i("Starting Background tests for primitive: {}", pmt);
        }
        for (int num: testNums) {
            for (int width: testWidths) {
                currentTest++;
                if (Comm::isClient()) {
                    Log::i("Progress: {}/{} - Testing Background primitive: {}, num: {}, width: {}",
                           currentTest, totalTests, pmt, num, width);
                }
                std::vector<int64_t> originsA, originsB, conditions;
                prepareOrigins(num, originsA, originsB, conditions);

                int64_t backgroundTime;
                if (!testBackground(testPmts, pmt, width, originsA, originsB, conditions, backgroundTime))
                    continue;

                // Collect timing data from servers
                if (Comm::isServer()) {
                    // Send timing data to client (rank 2)
                    Comm::send(backgroundTime, 64, 2, task);
                } else if (Comm::isClient()) {
                    // Client receives timing data from both servers (rank 0 and 1)
                    int64_t backgroundTime0, backgroundTime1;

                    Comm::receive(backgroundTime0, 64, 0, task);
                    Comm::receive(backgroundTime1, 64, 1, task);

                    // Calculate average time and store
                    double avgBackgroundTime = (backgroundTime0 + backgroundTime1) / 2.0;
                    std::string key = pmt + "_" + std::to_string(num) + "_" + std::to_string(width);
                    backgroundResults[key] = static_cast<int64_t>(avgBackgroundTime);

                    // Create result entry if we have both JIT and Background results
                    if (jitResults.find(key) != jitResults.end()) {
                        BenchmarkResult result;
                        result.primitive = pmt;
                        result.num = num;
                        result.width = width;
                        result.avgBackgroundTime = avgBackgroundTime;
                        result.avgJitTime = jitResults[key];
                        result.timestamp = timestamp;
                        results.push_back(result);
                    }

                    Log::i("Background - Primitive: {}, Num: {}, Width: {} - Server0: {}ms, Server1: {}ms, Average: {}ms",
                           pmt, num, width, backgroundTime0, backgroundTime1, avgBackgroundTime);
                }
            }
        }
    }

    // Write results to CSV file (only on client side)
    if (Comm::isClient() && !results.empty()) {
        std::string filename = "benchmark_bg_vs_jit_" + timestamp + ".csv";
        std::ofstream csvFile(filename);

        if (csvFile.is_open()) {
            // Write CSV header
            csvFile <<
                    "Timestamp,Primitive,NumElements,Width,AvgBackgroundTime_ms,AvgJitTime_ms,BackgroundVsJitRatio,JitVsBackgroundSpeedup\n";

            // Write data rows
            for (const auto &result: results) {
                double ratio = result.avgBackgroundTime / result.avgJitTime;
                double speedup = result.avgJitTime / result.avgBackgroundTime;

                csvFile << result.timestamp << ","
                        << result.primitive << ","
                        << result.num << ","
                        << result.width << ","
                        << std::fixed << std::setprecision(2) << result.avgBackgroundTime << ","
                        << std::fixed << std::setprecision(2) << result.avgJitTime << ","
                        << std::fixed << std::setprecision(4) << ratio << ","
                        << std::fixed << std::setprecision(4) << speedup << "\n";
            }

            csvFile.close();
            Log::i("Benchmark results saved to: {}", filename);
        } else {
            Log::e("Failed to open CSV file for writing: {}", filename);
        }
    }

    System::finalize();
}
