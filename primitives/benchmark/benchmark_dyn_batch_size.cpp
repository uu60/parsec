//
// Created by 杜建璋 on 25-7-31.
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
#include <algorithm>
#include <cfloat>
#include <string>

#include "compute/batch/bool/BoolEqualBatchOperator.h"
#include "compute/batch/bool/BoolMutexBatchOperator.h"
#include "compute/batch/bool/BoolToArithBatchOperator.h"
#include "parallel/ThreadPoolSupport.h"

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

bool test(std::vector<std::string> &testPmts, std::string pmt, int width, int batchSize, std::vector<int64_t> &originsA,
             std::vector<int64_t> &originsB, std::vector<int64_t> &conditions, int64_t &jitTime) {
    std::vector<int64_t> secretsA;
    std::vector<int64_t> secretsB;
    std::vector<int64_t> secretConditions;

    boolShare(width, originsA, originsB, conditions, secretsA, secretsB, secretConditions);

    Conf::BATCH_SIZE = batchSize;
    int batch_num = (static_cast<int>(secretsA.size()) + batchSize - 1) / batchSize;
    if (Comm::isServer()) {
        int64_t start = System::currentTimeMillis();
        if (pmt == testPmts[0]) {
            // "<"
            std::vector<int64_t> zs;
            zs.reserve(secretsA.size());
            std::vector<std::future<std::vector<int64_t> > > futures(batch_num);
            for (int b = 0; b < batch_num; ++b) {
                futures[b] = ThreadPoolSupport::submit([&, b] {
                    int startIdx = b * batchSize;
                    int endIdx = std::min((b + 1) * batchSize, static_cast<int>(secretsA.size()));
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
                    int startIdx = b * batchSize;
                    int endIdx = std::min((b + 1) * batchSize, static_cast<int>(secretsA.size()));
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
                    int startIdx = b * batchSize;
                    int endIdx = std::min((b + 1) * batchSize, static_cast<int>(secretsA.size()));
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
                    int startIdx = b * batchSize;
                    int endIdx = std::min((b + 1) * batchSize, static_cast<int>(secretsA.size()));
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
                    int startIdx = b * batchSize;
                    int endIdx = std::min((b + 1) * batchSize, static_cast<int>(secretsA.size()));
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
            // "ar"
            std::vector<std::future<void > > futures(batch_num);
            std::atomic_int64_t sumShare = 0;
            int64_t sumShare1;
            for (int b = 0; b < batch_num; ++b) {
                futures[b] = ThreadPoolSupport::submit([&, b] {
                    int startIdx = b * batchSize;
                    int endIdx = std::min((b + 1) * batchSize, static_cast<int>(secretsA.size()));
                    std::vector<int64_t> batchN(secretConditions.begin() + startIdx,
                                                secretConditions.begin() + endIdx);
                    auto ta = BoolToArithBatchOperator(&batchN, 64, 0, 0,
                                                       SecureOperator::NO_CLIENT_COMPUTE).
                            execute()->_zis;
                    sumShare += std::accumulate(ta.begin(), ta.end(), 0ll);
                });
            }
            for (auto &f : futures) {
                f.wait();
            }
            auto r0 = Comm::serverSendAsync(sumShare, 32, 0);
            auto r1 = Comm::serverReceiveAsync(sumShare1, 32, 0);
            Comm::wait(r0);
            Comm::wait(r1);

            int64_t validNum = sumShare + sumShare1;
        } else if (pmt == testPmts[6]) {
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
        int batchSize{};
        double avgExecutionTime{};
        double throughput{};
        double timePerOperation{};
        double timePerBatch{};
        std::string timestamp;
    };

    std::vector<BenchmarkResult> results;

    // Generate timestamp for filename
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    std::stringstream ss;
    ss << std::put_time(std::localtime(&time_t), "%Y%m%d_%H%M%S");
    std::string timestamp = ss.str();

    std::vector testNums = {1000, 10000, 100000, 1000000};
    std::vector testWidths = {1, 2, 4, 8, 16, 32, 64,};
    std::vector<std::string> testPmts = {"<", "<=", "==", "!=", "mux", "ar", "sort"};
    std::vector<int> testBatchSizes = {1, 10, 100, 1000, 10000, 100000, 1000000, 10000000};

    // Calculate total number of tests for progress tracking
    int totalTests = 0;
    for (int num: testNums) {
        for (int batchSize: testBatchSizes) {
            if (batchSize <= num) {
                totalTests += testPmts.size() * testWidths.size();
            }
        }
    }
    int currentTest = 0;

    // Run tests for different batch sizes
    if (Comm::isClient()) {
        Log::i("Starting Dynamic Batch Size Benchmark...");
    }
    
    for (std::string pmt: testPmts) {
        if (Comm::isClient()) {
            Log::i("Testing primitive: {}", pmt);
        }
        for (int num: testNums) {
            for (int width: testWidths) {
                if (Comm::isClient()) {
                    Log::i("Testing num: {}, width: {}", num, width);
                }
                
                // Prepare test data once for all batch sizes
                std::vector<int64_t> originsA, originsB, conditions;
                prepareOrigins(num, originsA, originsB, conditions);
                
                for (int batchSize: testBatchSizes) {
                    // Skip batch sizes larger than the number of elements
                    if (batchSize > num) continue;
                    
                    currentTest++;
                    if (Comm::isClient()) {
                        Log::i("Progress: {}/{} - Testing primitive: {}, num: {}, width: {}, batchSize: {}",
                               currentTest, totalTests, pmt, num, width, batchSize);
                    }

                    int64_t executionTime;
                    if (!test(testPmts, pmt, width, batchSize, originsA, originsB, conditions, executionTime))
                        continue;

                    // Collect timing data from servers
                    if (Comm::isServer()) {
                        // Send timing data to client (rank 2)
                        Comm::send(executionTime, 64, 2, 0);
                    } else if (Comm::isClient()) {
                        // Client receives timing data from both servers (rank 0 and 1)
                        int64_t time0, time1;

                        Comm::receive(time0, 64, 0, 0);
                        Comm::receive(time1, 64, 1, 0);

                        // Calculate average time and store result
                        double avgTime = (time0 + time1) / 2.0;
                        
                        BenchmarkResult result;
                        result.primitive = pmt;
                        result.num = num;
                        result.width = width;
                        result.batchSize = batchSize;
                        result.avgExecutionTime = avgTime;
                        result.timestamp = timestamp;
                        
                        // Calculate throughput (operations per second)
                        result.throughput = (num * 1000.0) / avgTime; // ops/sec
                        
                        // Calculate efficiency metrics
                        result.timePerOperation = avgTime / num; // ms per operation
                        result.timePerBatch = avgTime / ((num + batchSize - 1) / batchSize); // ms per batch
                        
                        results.push_back(result);

                        Log::i("Primitive: {}, Num: {}, Width: {}, BatchSize: {} - Server0: {}ms, Server1: {}ms, Average: {}ms, Throughput: {} ops/sec",
                               pmt, num, width, batchSize, time0, time1, avgTime, result.throughput);
                    }
                }
            }
        }
    }

    // Write results to CSV file (only on client side)
    if (Comm::isClient() && !results.empty()) {
        std::string filename = "benchmark_dyn_batch_size_" + timestamp + ".csv";
        std::ofstream csvFile(filename);

        if (csvFile.is_open()) {
            // Write CSV header
            csvFile << "Timestamp,Primitive,NumElements,Width,BatchSize,AvgExecutionTime_ms,Throughput_ops_per_sec,TimePerOperation_ms,TimePerBatch_ms\n";

            // Write data rows
            for (const auto &result: results) {
                csvFile << result.timestamp << ","
                        << result.primitive << ","
                        << result.num << ","
                        << result.width << ","
                        << result.batchSize << ","
                        << std::fixed << std::setprecision(2) << result.avgExecutionTime << ","
                        << std::fixed << std::setprecision(2) << result.throughput << ","
                        << std::fixed << std::setprecision(6) << result.timePerOperation << ","
                        << std::fixed << std::setprecision(4) << result.timePerBatch << "\n";
            }

            csvFile.close();
            Log::i("Benchmark results saved to: {}", filename);
        } else {
            Log::e("Failed to open CSV file for writing: {}", filename);
        }
    }

    // Final output summary
    if (Comm::isClient()) {
        Log::i("=== Dynamic Batch Size Benchmark Completed ===");
        Log::i("Total tests executed: {}", currentTest);
        Log::i("Total primitives tested: {}", testPmts.size());
        Log::i("Total configurations tested: {}", results.size());
        
        if (!results.empty()) {
            // Find best overall performance
            auto bestResult = std::max_element(results.begin(), results.end(),
                [](const BenchmarkResult &a, const BenchmarkResult &b) {
                    return a.throughput < b.throughput;
                });
            
            Log::i("Best performance: {} with {} elements, width {}, batch size {} - {} ops/sec",
                   bestResult->primitive, bestResult->num, bestResult->width, 
                   bestResult->batchSize, bestResult->throughput);
            
            // Calculate overall statistics
            double totalThroughput = 0.0;
            double totalTime = 0.0;
            for (const auto &result : results) {
                totalThroughput += result.throughput;
                totalTime += result.avgExecutionTime;
            }
            
            Log::i("Average throughput across all tests: {} ops/sec", totalThroughput / results.size());
            Log::i("Average execution time across all tests: {} ms", totalTime / results.size());
        }
        
        Log::i("Results saved to CSV file with timestamp: {}", timestamp);
    }

    System::finalize();
}
