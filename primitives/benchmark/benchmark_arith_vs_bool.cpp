//
// Created by 杜建璋 on 25-7-16.
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

#include "compute/batch/arith/ArithEqualBatchOperator.h"
#include "compute/batch/arith/ArithMutexBatchOperator.h"
#include "compute/batch/bool/BoolEqualBatchOperator.h"
#include "compute/batch/bool/BoolMutexBatchOperator.h"
#include "compute/batch/bool/BoolToArithBatchOperator.h"

#include <string>
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

bool testArith(std::vector<std::string> &testPmts, std::string pmt, int width, std::vector<int64_t> &originsA,
               std::vector<int64_t> &originsB, std::vector<int64_t> &conditions, int64_t &arithTime) {
    std::vector<int64_t> secretsA;
    std::vector<int64_t> secretsB;
    std::vector<int64_t> secretConditions;

    arithShare(width, originsA, originsB, conditions, secretsA, secretsB, secretConditions);

    if (Comm::isServer()) {
        int64_t start = System::currentTimeMillis();
        if (pmt == testPmts[0]) {
            // "<"
            ArithLessBatchOperator(&secretsA, &secretsB, width, 0, 0, -1).execute();
        } else if (pmt == testPmts[1]) {
            // "<="
            auto tempZ = ArithLessBatchOperator(&secretsB, &secretsA, width, 0, 0, -1).
                    execute()
                    ->_zis;
            for (auto &t: tempZ) {
                t = t ^ Comm::rank();
            }
        } else if (pmt == testPmts[2]) {
            // "=="
            ArithEqualBatchOperator(&secretsA, &secretsB, width, 0, 0, -1).execute();
        } else if (pmt == testPmts[3]) {
            // "!="
            auto tempZ = ArithEqualBatchOperator(&secretsA, &secretsB, width, 0, 0, -2).
                    execute()->_zis;
            for (auto &t: tempZ) {
                t = t ^ Comm::rank();
            }
        } else if (pmt == testPmts[4]) {
            // "mux"
            ArithMutexBatchOperator(&secretsA, &secretsB, &secretConditions, width, 0, 0, -1)
                    .execute();
        } else if (pmt == testPmts[5]) {
            // "ar"
            int64_t sumShare = 0, sumShare1;
            auto ta = BoolToArithBatchOperator(&secretConditions, 64, 0, 0,
                                               SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
            sumShare = std::accumulate(ta.begin(), ta.end(), 0ll);
        } else if (pmt == testPmts[6]) {
            // "sort"
            std::vector<ArithSecret> arithSecrets;
            arithSecrets.reserve(secretsA.size());
            for (size_t i = 0; i < secretsA.size(); i++) {
                arithSecrets.emplace_back(secretsA[i], width, 0);
            }
            start = System::currentTimeMillis();
            Secrets::sort(arithSecrets, true, 0);
        } else {
            Log::e("Unknown primitive: {}", pmt);
            return false;
        }
        arithTime = System::currentTimeMillis() - start;
    }
    return true;
}

bool testBool(std::vector<std::string> &testPmt, std::string pmt, int width, std::vector<int64_t> &originsA,
              std::vector<int64_t> &originsB, std::vector<int64_t> &conditions, int64_t &boolTime) {
    std::vector<int64_t> secretsA;
    std::vector<int64_t> secretsB;
    std::vector<int64_t> secretConditions;

    boolShare(width, originsA, originsB, conditions, secretsA, secretsB, secretConditions);

    if (Comm::isServer()) {
        int64_t start = System::currentTimeMillis();
        if (pmt == testPmt[0]) {
            // "<"
            BoolLessBatchOperator(&secretsA, &secretsB, width, 0, 0, -1).execute();
        } else if (pmt == testPmt[1]) {
            // "<="
            auto tempZ = BoolLessBatchOperator(&secretsB, &secretsA, width, 0, 0, -1).
                    execute()
                    ->_zis;
            for (auto &t: tempZ) {
                t = t ^ Comm::rank();
            }
        } else if (pmt == testPmt[2]) {
            // "=="
            BoolEqualBatchOperator(&secretsA, &secretsB, width, 0, 0, -1).execute();
        } else if (pmt == testPmt[3]) {
            // "!="
            auto tempZ = BoolEqualBatchOperator(&secretsA, &secretsB, width, 0, 0, -2).
                    execute()->_zis;
            for (auto &t: tempZ) {
                t = t ^ Comm::rank();
            }
        } else if (pmt == testPmt[4]) {
            // "mux"
            BoolMutexBatchOperator(&secretsA, &secretsB, &secretConditions, width, 0, 0, -1)
                    .execute();
        } else if (pmt == testPmt[5]) {
            // "ar"
            int64_t sumShare = 0, sumShare1;
            auto ta = BoolToArithBatchOperator(&secretConditions, 64, 0, 0,
                                               SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
            sumShare = std::accumulate(ta.begin(), ta.end(), 0ll);
        } else if (pmt == testPmt[6]) {
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
        boolTime = System::currentTimeMillis() - start;
    }
    return true;
}

// Function to parse comma-separated string into vector of integers
std::vector<int> parseCommaSeparatedInts(const std::string& str) {
    std::vector<int> result;
    std::stringstream ss(str);
    std::string item;
    
    while (std::getline(ss, item, ',')) {
        // Trim whitespace
        item.erase(0, item.find_first_not_of(" \t"));
        item.erase(item.find_last_not_of(" \t") + 1);
        
        if (!item.empty()) {
            try {
                result.push_back(std::stoi(item));
            } catch (const std::exception& e) {
                Log::e("Invalid number in input: {}", item);
            }
        }
    }
    return result;
}

int main(int argc, char *argv[]) {
    System::init(argc, argv);

    // CSV data collection structure
    struct BenchmarkResult {
        std::string primitive;
        int num{};
        int width{};
        double avgArithTime{};
        double avgBoolTime{};
        std::string timestamp;
    };

    std::vector<BenchmarkResult> results;

    // Generate timestamp for filename
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    std::stringstream ss;
    ss << std::put_time(std::localtime(&time_t), "%Y%m%d_%H%M%S");
    std::string timestamp = ss.str();

    // Default values
    std::vector<int> testNums = {1000, 10000, 100000};
    std::vector<int> testSortNums = {1000, 10000, 100000}; // Separate data sizes for sort
    std::vector<int> testWidths = {1, 2, 4, 8, 16, 32, 64};
    std::vector<std::string> testPmts = {"<", "<=", "==", "!=", "mux", "ar", "sort"};

    // Read data scales from command line using Conf::_userParams (like db/exp)
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

    // Read sort-specific data scales from command line
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
        // If sort_nums not specified, use same as nums
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

    // Calculate total number of tests for progress tracking
    int totalTests = 0;
    for (const std::string& pmt : testPmts) {
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
        
        // Choose appropriate data sizes based on primitive type
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

                // Collect timing data from servers and compute average on client
                if (Comm::isServer()) {
                    // Send timing data to client (rank 2)
                    Comm::send(arithTime, 64, 2, 0);
                    Comm::send(boolTime, 64, 2, 1);
                } else if (Comm::isClient()) {
                    // Client receives timing data from both servers (rank 0 and 1)
                    int64_t arithTime0, arithTime1, boolTime0, boolTime1;

                    Comm::receive(arithTime0, 64, 0, 0);
                    Comm::receive(boolTime0, 64, 0, 1);
                    Comm::receive(arithTime1, 64, 1, 0);
                    Comm::receive(boolTime1, 64, 1, 1);

                    // Calculate average times
                    double avgArithTime = (arithTime0 + arithTime1) / 2.0;
                    double avgBoolTime = (boolTime0 + boolTime1) / 2.0;

                    // Store result for CSV output
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

    // Write results to CSV file (only on client side)
    if (Comm::isClient() && !results.empty()) {
        std::string filename = "benchmark_arith_vs_bool_" + timestamp + ".csv";
        std::ofstream csvFile(filename);

        if (csvFile.is_open()) {
            // Write CSV header
            csvFile <<
                    "Timestamp,Primitive,NumElements,Width,AvgArithTime_ms,AvgBoolTime_ms,ArithVsBoolRatio,BoolVsArithSpeedup\n";

            // Write data rows
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
