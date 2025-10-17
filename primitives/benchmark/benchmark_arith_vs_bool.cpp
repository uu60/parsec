
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

#include "compute/batch/arith/ArithEqualBatchOperator.h"
#include "compute/batch/arith/ArithMutexBatchOperator.h"
#include "compute/batch/bool/BoolEqualBatchOperator.h"
#include "compute/batch/bool/BoolMutexBatchOperator.h"
#include "compute/batch/bool/BoolToArithBatchOperator.h"

#include <string>

#include "parallel/ThreadPoolSupport.h"

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
}

void arithShare(int width, std::vector<int64_t> &originsA, std::vector<int64_t> &originsB,
                std::vector<int64_t> &conditions, std::vector<int64_t> &secretsA,
                std::vector<int64_t> &secretsB, std::vector<int64_t> &secretConditions) {
    secretsA = Secrets::arithShare(originsA, 2, width, 0);
    secretsB = Secrets::arithShare(originsB, 2, width, 0);
    secretConditions = Secrets::arithShare(conditions, 2, 1, 0);
}

void boolShare(int width, std::vector<int64_t> &originsA, std::vector<int64_t> &originsB,
               std::vector<int64_t> &conditions, std::vector<int64_t> &secretsA,
               std::vector<int64_t> &secretsB, std::vector<int64_t> &secretConditions) {
    secretsA = Secrets::boolShare(originsA, 2, width, 0);
    secretsB = Secrets::boolShare(originsB, 2, width, 0);
    secretConditions = Secrets::boolShare(conditions, 2, 1, 0);
}

bool testArith(std::vector<std::string> &testPmts, std::string pmt, int width,
               std::vector<int64_t> &originsA, std::vector<int64_t> &originsB,
               std::vector<int64_t> &conditions, int64_t &arithTime) {
    std::vector<int64_t> secretsA, secretsB, secretConditions;
    arithShare(width, originsA, originsB, conditions, secretsA, secretsB, secretConditions);

    int batch_size = Conf::BATCH_SIZE;
    int batch_num = (static_cast<int>(secretsA.size()) + batch_size - 1) / batch_size;

    if (Comm::isServer()) {
        int64_t start = System::currentTimeMillis();

        if (pmt == testPmts[0]) {
            std::vector<int64_t> zs;
            zs.reserve(secretsA.size());
            std::vector<std::future<std::vector<int64_t> > > futures(batch_num);
            for (int b = 0; b < batch_num; ++b) {
                int startIdx = b * batch_size;
                int endIdx = std::min((b + 1) * batch_size, (int) secretsA.size());
                futures[b] = ThreadPoolSupport::submit([&, b, startIdx, endIdx] {
                    std::vector<int64_t> batchA(secretsA.begin() + startIdx, secretsA.begin() + endIdx);
                    std::vector<int64_t> batchB(secretsB.begin() + startIdx, secretsB.begin() + endIdx);
                    return ArithLessBatchOperator(&batchA, &batchB, width, 0, b * 1024, -1)
                            .execute()->_zis;
                });
            }
            for (auto &f: futures) {
                auto v = f.get();
                zs.insert(zs.end(), v.begin(), v.end());
            }
        } else if (pmt == testPmts[1]) {
            std::vector<int64_t> temp;
            temp.reserve(secretsA.size());
            std::vector<std::future<std::vector<int64_t> > > futures(batch_num);
            for (int b = 0; b < batch_num; ++b) {
                int s = b * batch_size, e = std::min((b + 1) * batch_size, (int) secretsA.size());
                futures[b] = ThreadPoolSupport::submit([&, b, s, e] {
                    std::vector<int64_t> batchA(secretsA.begin() + s, secretsA.begin() + e);
                    std::vector<int64_t> batchB(secretsB.begin() + s, secretsB.begin() + e);
                    return ArithLessBatchOperator(&batchB, &batchA, width, 0, b * 2048, -1).execute()->_zis;
                });
            }
            for (auto &f: futures) {
                auto v = f.get();
                temp.insert(temp.end(), v.begin(), v.end());
            }
            for (auto &t: temp) t ^= Comm::rank();
        } else if (pmt == testPmts[2]) {
            std::vector<int64_t> zs;
            zs.reserve(secretsA.size());
            std::vector<std::future<std::vector<int64_t> > > futures(batch_num);
            for (int b = 0; b < batch_num; ++b) {
                int s = b * batch_size, e = std::min((b + 1) * batch_size, (int) secretsA.size());
                futures[b] = ThreadPoolSupport::submit([&, b, s, e] {
                    std::vector<int64_t> batchA(secretsA.begin() + s, secretsA.begin() + e);
                    std::vector<int64_t> batchB(secretsB.begin() + s, secretsB.begin() + e);
                    return ArithEqualBatchOperator(&batchA, &batchB, width, 0, b * 3072, -1).execute()->_zis;
                });
            }
            for (auto &f: futures) {
                auto v = f.get();
                zs.insert(zs.end(), v.begin(), v.end());
            }
        } else if (pmt == testPmts[3]) {
            std::vector<int64_t> temp;
            temp.reserve(secretsA.size());
            std::vector<std::future<std::vector<int64_t> > > futures(batch_num);
            for (int b = 0; b < batch_num; ++b) {
                int s = b * batch_size, e = std::min((b + 1) * batch_size, (int) secretsA.size());
                futures[b] = ThreadPoolSupport::submit([&, b, s, e] {
                    std::vector<int64_t> batchA(secretsA.begin() + s, secretsA.begin() + e);
                    std::vector<int64_t> batchB(secretsB.begin() + s, secretsB.begin() + e);
                    return ArithEqualBatchOperator(&batchA, &batchB, width, 0, b * 4096, -2).execute()->_zis;
                });
            }
            for (auto &f: futures) {
                auto v = f.get();
                temp.insert(temp.end(), v.begin(), v.end());
            }
            for (auto &t: temp) t ^= Comm::rank();
        } else if (pmt == testPmts[4]) {
            std::vector<std::future<void> > futures(batch_num);
            for (int b = 0; b < batch_num; ++b) {
                int s = b * batch_size, e = std::min((b + 1) * batch_size, (int) secretsA.size());
                futures[b] = ThreadPoolSupport::submit([&, b, s, e] {
                    std::vector<int64_t> A(secretsA.begin() + s, secretsA.begin() + e);
                    std::vector<int64_t> B(secretsB.begin() + s, secretsB.begin() + e);
                    std::vector<int64_t> C(secretConditions.begin() + s, secretConditions.begin() + e);
                    ArithMutexBatchOperator(&A, &B, &C, width, 0, b * 5120, -1).execute();
                });
            }
            for (auto &f: futures) f.get();
        } else if (pmt == testPmts[5]) {
            std::vector<int64_t> results_ar;
            results_ar.reserve(secretConditions.size());
            std::vector<std::future<std::vector<int64_t> > > futures(batch_num);
            for (int b = 0; b < batch_num; ++b) {
                int s = b * batch_size, e = std::min((b + 1) * batch_size, (int) secretConditions.size());
                futures[b] = ThreadPoolSupport::submit([&, b, s, e] {
                    std::vector<int64_t> batchConditions(secretConditions.begin() + s, secretConditions.begin() + e);
                    return BoolToArithBatchOperator(&batchConditions, 64, 0, b * 6144,
                                                    SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
                });
            }
            for (auto &f: futures) {
                auto v = f.get();
                results_ar.insert(results_ar.end(), v.begin(), v.end());
            }
            volatile int64_t sink = std::accumulate(results_ar.begin(), results_ar.end(), 0ll);
            (void) sink;
        } else if (pmt == testPmts[6]) {
            std::vector<ArithSecret> as;
            as.reserve(secretsA.size());
            for (size_t i = 0; i < secretsA.size(); ++i) as.emplace_back(secretsA[i], width, 0, 0);
            Secrets::sort(as, true, 0);
        } else {
            Log::e("Unknown primitive: {}", pmt);
            return false;
        }

        arithTime = System::currentTimeMillis() - start;
    }
    return true;
}

bool testBool(std::vector<std::string> &testPmt, std::string pmt, int width,
              std::vector<int64_t> &originsA, std::vector<int64_t> &originsB,
              std::vector<int64_t> &conditions, int64_t &boolTime) {
    std::vector<int64_t> secretsA, secretsB, secretConditions;
    boolShare(width, originsA, originsB, conditions, secretsA, secretsB, secretConditions);

    const int batch_size = Conf::BATCH_SIZE;
    const int total = static_cast<int>(secretsA.size());
    const int batch_num = (total + batch_size - 1) / batch_size;

    if (Comm::isServer()) {
        const int64_t start = System::currentTimeMillis();

        if (pmt == testPmt[0]) {
            std::vector<int64_t> zs;
            zs.reserve(total);
            std::vector<std::future<std::vector<int64_t> > > futures(batch_num);
            for (int b = 0; b < batch_num; ++b) {
                const int s = b * batch_size;
                const int e = std::min((b + 1) * batch_size, total);
                futures[b] = ThreadPoolSupport::submit([&, b, s, e] {
                    std::vector<int64_t> A(secretsA.begin() + s, secretsA.begin() + e);
                    std::vector<int64_t> B(secretsB.begin() + s, secretsB.begin() + e);
                    return BoolLessBatchOperator(
                                &A, &B, width,
                                0,
                                BoolLessBatchOperator::tagStride() * b,
                                -1)
                            .execute()->_zis;
                });
            }
            for (auto &f: futures) {
                auto part = f.get();
                zs.insert(zs.end(), part.begin(), part.end());
            }
        } else if (pmt == testPmt[1]) {
            std::vector<int64_t> temp;
            temp.reserve(total);
            std::vector<std::future<std::vector<int64_t> > > futures(batch_num);
            for (int b = 0; b < batch_num; ++b) {
                const int s = b * batch_size;
                const int e = std::min((b + 1) * batch_size, total);
                futures[b] = ThreadPoolSupport::submit([&, b, s, e] {
                    std::vector<int64_t> A(secretsA.begin() + s, secretsA.begin() + e);
                    std::vector<int64_t> B(secretsB.begin() + s, secretsB.begin() + e);
                    return BoolLessBatchOperator(
                                &B, &A, width,
                                0,
                                BoolLessBatchOperator::tagStride() * b,
                                -1)
                            .execute()->_zis;
                });
            }
            for (auto &f: futures) {
                auto part = f.get();
                temp.insert(temp.end(), part.begin(), part.end());
            }
            for (auto &t: temp) t ^= Comm::rank();
        } else if (pmt == testPmt[2]) {
            std::vector<int64_t> zs;
            zs.reserve(total);
            std::vector<std::future<std::vector<int64_t> > > futures(batch_num);
            for (int b = 0; b < batch_num; ++b) {
                const int s = b * batch_size;
                const int e = std::min((b + 1) * batch_size, total);
                futures[b] = ThreadPoolSupport::submit([&, b, s, e] {
                    std::vector<int64_t> A(secretsA.begin() + s, secretsA.begin() + e);
                    std::vector<int64_t> B(secretsB.begin() + s, secretsB.begin() + e);
                    return BoolEqualBatchOperator(
                                &A, &B, width,
                                0,
                                BoolEqualBatchOperator::tagStride() * b,
                                -1)
                            .execute()->_zis;
                });
            }
            for (auto &f: futures) {
                auto part = f.get();
                zs.insert(zs.end(), part.begin(), part.end());
            }
        } else if (pmt == testPmt[3]) {
            std::vector<int64_t> temp;
            temp.reserve(total);
            std::vector<std::future<std::vector<int64_t> > > futures(batch_num);
            for (int b = 0; b < batch_num; ++b) {
                const int s = b * batch_size;
                const int e = std::min((b + 1) * batch_size, total);
                futures[b] = ThreadPoolSupport::submit([&, b, s, e] {
                    std::vector<int64_t> A(secretsA.begin() + s, secretsA.begin() + e);
                    std::vector<int64_t> B(secretsB.begin() + s, secretsB.begin() + e);
                    return BoolEqualBatchOperator(
                                &A, &B, width,
                                0,
                                BoolEqualBatchOperator::tagStride() * b,
                                -2)
                            .execute()->_zis;
                });
            }
            for (auto &f: futures) {
                auto part = f.get();
                temp.insert(temp.end(), part.begin(), part.end());
            }
            for (auto &t: temp) t ^= Comm::rank();
        } else if (pmt == testPmt[4]) {
            std::vector<std::future<void> > futures(batch_num);
            for (int b = 0; b < batch_num; ++b) {
                const int s = b * batch_size;
                const int e = std::min((b + 1) * batch_size, total);
                futures[b] = ThreadPoolSupport::submit([&, b, s, e] {
                    std::vector<int64_t> A(secretsA.begin() + s, secretsA.begin() + e);
                    std::vector<int64_t> B(secretsB.begin() + s, secretsB.begin() + e);
                    std::vector<int64_t> C(secretConditions.begin() + s, secretConditions.begin() + e);
                    BoolMutexBatchOperator(
                        &A, &B, &C, width,
                        0,
                        BoolMutexBatchOperator::tagStride() * b,
                        -1
                    ).execute();
                });
            }
            for (auto &f: futures) f.get();
        } else if (pmt == testPmt[5]) {
            std::vector<int64_t> results_ar;
            results_ar.reserve(secretConditions.size());
            std::vector<std::future<std::vector<int64_t> > > futures(batch_num);
            for (int b = 0; b < batch_num; ++b) {
                const int s = b * batch_size;
                const int e = std::min((b + 1) * batch_size, total);
                futures[b] = ThreadPoolSupport::submit([&, b, s, e] {
                    std::vector<int64_t> batchConditions(secretConditions.begin() + s, secretConditions.begin() + e);
                    return BoolToArithBatchOperator(&batchConditions, 64, 0, b * 6144,
                                                    SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
                });
            }
            for (auto &f: futures) {
                auto part = f.get();
                results_ar.insert(results_ar.end(), part.begin(), part.end());
            }
            volatile int64_t sink = std::accumulate(results_ar.begin(), results_ar.end(), 0ll);
            (void) sink;
        } else if (pmt == testPmt[6]) {
            std::vector<BoolSecret> bs;
            bs.reserve(secretsA.size());
            for (size_t i = 0; i < secretsA.size(); ++i) {
                bs.emplace_back(secretsA[i], width, 0, 0);
            }
            Secrets::sort(bs, true, 0);
        } else {
            Log::e("Unknown primitive: {}", pmt);
            return false;
        }

        boolTime = System::currentTimeMillis() - start;
    }

    return true;
}

std::vector<int> parseCommaSeparatedInts(const std::string &str) {
    std::vector<int> result;
    std::stringstream ss(str);
    std::string item;

    while (std::getline(ss, item, ',')) {
        item.erase(0, item.find_first_not_of(" \t"));
        item.erase(item.find_last_not_of(" \t") + 1);

        if (!item.empty()) {
            try {
                result.push_back(std::stoi(item));
            } catch (const std::exception &e) {
                Log::e("Invalid number in input: {}", item);
            }
        }
    }
    return result;
}

int main(int argc, char *argv[]) {
    System::init(argc, argv);

    struct BenchmarkResult {
        std::string primitive;
        int num{};
        int width{};
        double avgArithTime{};
        double avgBoolTime{};
        std::string timestamp;
    };

    std::vector<BenchmarkResult> results;

    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    std::stringstream ss;
    ss << std::put_time(std::localtime(&time_t), "%Y%m%d_%H%M%S");
    std::string timestamp = ss.str();

    std::vector<int> testNums = {1000, 10000, 100000};
    std::vector<int> testSortNums = {1000, 10000, 100000};
    std::vector<int> testWidths = {1, 2, 4, 8, 16, 32, 64};
    std::vector<std::string> testPmts = {"<", "<=", "==", "!=", "mux", "ar", "sort"};

    if (Conf::_userParams.count("nums")) {
        std::string numsStr = Conf::_userParams["nums"];
        testNums = parseCommaSeparatedInts(numsStr);
        if (testNums.empty()) {
            if (Comm::isClient()) {
                Log::e("Invalid or empty nums parameter: {}", numsStr);
                Log::i("Usage: --nums=1000,10000,100000 --sort_nums=500,1000,5000 [--widths=1,2,4,8,16,32,64]");
            }
            System::finalize();
            return 1;
        }
    }

    if (Conf::_userParams.count("sort_nums")) {
        std::string sortNumsStr = Conf::_userParams["sort_nums"];
        testSortNums = parseCommaSeparatedInts(sortNumsStr);
        if (testSortNums.empty()) {
            if (Comm::isClient()) {
                Log::e("Invalid or empty sort_nums parameter: {}", sortNumsStr);
            }
            System::finalize();
            return 1;
        }
    } else {
        testSortNums = testNums;
    }

    if (Conf::_userParams.count("widths")) {
        std::string widthsStr = Conf::_userParams["widths"];
        testWidths = parseCommaSeparatedInts(widthsStr);
        if (testWidths.empty()) {
            if (Comm::isClient()) {
                Log::e("Invalid or empty widths parameter: {}", widthsStr);
            }
            System::finalize();
            return 1;
        }
    }

    if (Comm::isClient()) {
        Log::i("Starting benchmark with data scales for non-sort operations: ");
        for (size_t i = 0; i < testNums.size(); i++) {
            Log::i("  {}{}", testNums[i], (i == testNums.size() - 1) ? "" : ",");
        }
        Log::i("Data scales for sort operations: ");
        for (size_t i = 0; i < testSortNums.size(); i++) {
            Log::i("  {}{}", testSortNums[i], (i == testSortNums.size() - 1) ? "" : ",");
        }
        Log::i("Using widths: ");
        for (size_t i = 0; i < testWidths.size(); i++) {
            Log::i("  {}{}", testWidths[i], (i == testWidths.size() - 1) ? "" : ",");
        }
    }

    int totalTests = 0;
    for (const std::string &pmt: testPmts) {
        if (pmt == "sort") {
            totalTests += testSortNums.size() * testWidths.size();
        } else {
            totalTests += testNums.size() * testWidths.size();
        }
    }
    int currentTest = 0;

    for (std::string pmt: testPmts) {
        if (Comm::isClient()) {
            Log::i("Starting tests for primitive: {}", pmt);
        }

        std::vector<int> currentNums = (pmt == "sort") ? testSortNums : testNums;

        for (int num: currentNums) {
            for (int width: testWidths) {
                currentTest++;
                if (Comm::isClient()) {
                    Log::i("Progress: {}/{} - Testing primitive: {}, num: {}, width: {}",
                           currentTest, totalTests, pmt, num, width);
                }
                std::vector<int64_t> originsA, originsB, conditions;
                prepareOrigins(num, originsA, originsB, conditions);

                int64_t arithTime;
                if (!testArith(testPmts, pmt, width, originsA, originsB, conditions, arithTime))
                    continue;

                int64_t boolTime;
                if (!testBool(testPmts, pmt, width, originsA, originsB, conditions, boolTime))
                    continue;

                if (Comm::isServer()) {
                    Comm::send(arithTime, 64, 2, 0);
                    Comm::send(boolTime, 64, 2, 1);
                } else if (Comm::isClient()) {
                    int64_t arithTime0, arithTime1, boolTime0, boolTime1;

                    Comm::receive(arithTime0, 64, 0, 0);
                    Comm::receive(boolTime0, 64, 0, 1);
                    Comm::receive(arithTime1, 64, 1, 0);
                    Comm::receive(boolTime1, 64, 1, 1);

                    double avgArithTime = (arithTime0 + arithTime1) / 2.0;
                    double avgBoolTime = (boolTime0 + boolTime1) / 2.0;

                    BenchmarkResult result;
                    result.primitive = pmt;
                    result.num = num;
                    result.width = width;
                    result.avgArithTime = avgArithTime;
                    result.avgBoolTime = avgBoolTime;
                    result.timestamp = timestamp;
                    results.push_back(result);

                    Log::i("Primitive: {}, Num: {}, Width: {} - Server0 - ArithTime: {}ms, BoolTime: {}ms",
                           pmt, num, width, arithTime0, boolTime0);
                    Log::i("Primitive: {}, Num: {}, Width: {} - Server1 - ArithTime: {}ms, BoolTime: {}ms",
                           pmt, num, width, arithTime1, boolTime1);
                    Log::i("Primitive: {}, Num: {}, Width: {} - Average - ArithTime: {}ms, BoolTime: {}ms",
                           pmt, num, width, avgArithTime, avgBoolTime);
                }
            }
        }
    }

    if (Comm::isClient() && !results.empty()) {
        std::string filename = "benchmark_arith_vs_bool_" + timestamp + ".csv";
        std::ofstream csvFile(filename);

        if (csvFile.is_open()) {
            csvFile <<
                    "Timestamp,Primitive,NumElements,Width,AvgArithTime_ms,AvgBoolTime_ms,ArithVsBoolRatio,BoolVsArithSpeedup\n";

            for (const auto &result: results) {
                double ratio = result.avgArithTime / result.avgBoolTime;
                double speedup = result.avgBoolTime / result.avgArithTime;

                csvFile << result.timestamp << ","
                        << result.primitive << ","
                        << result.num << ","
                        << result.width << ","
                        << std::fixed << std::setprecision(2) << result.avgArithTime << ","
                        << std::fixed << std::setprecision(2) << result.avgBoolTime << ","
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
