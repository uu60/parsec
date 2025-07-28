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

void prepareOrigins(int num, std::vector<int64_t> originsA, std::vector<int64_t> originsB) {
    // arith compare less than
    if (Comm::isClient()) {
        originsA.resize(num);
        originsB.resize(num);
        for (int i = 0; i < num; i++) {
            originsA[i] = Math::randInt(0, num);
            originsB[i] = Math::randInt(0, num);
        }
    }
}

void arithShare(int width, std::vector<int64_t> originsA, std::vector<int64_t> originsB, std::vector<int64_t> &secretsA,
                std::vector<int64_t> &secretsB) {
    secretsA = Secrets::arithShare(originsA, 2, width, 0);
    secretsB = Secrets::arithShare(originsB, 2, width, 0);
}

void boolShare(int width, std::vector<int64_t> originsA, std::vector<int64_t> originsB, std::vector<int64_t> &secretsA,
               std::vector<int64_t> &secretsB) {
    secretsA = Secrets::boolShare(originsA, 2, width, 0);
    secretsB = Secrets::boolShare(originsB, 2, width, 0);
}

int main(int argc, char *argv[]) {
    System::init(argc, argv);

    // CSV data collection structure
    struct BenchmarkResult {
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

    std::vector testNums = {1000, 10000, 100000, 1000000};
    std::vector testWidths = {1, 2, 4, 8, 16, 32, 64,};
    std::vector testPmt = {"<, <=, ==, !=, sort"};

    for (std::string pmt: testPmt) {
        for (int num: testNums) {
            for (int width: testWidths) {
                std::vector<int64_t> originsA, originsB;

                prepareOrigins(num, originsA, originsB);

                std::vector<int64_t> secretsA;
                std::vector<int64_t> secretsB;
                arithShare(width, originsA, originsB, secretsA, secretsB);

                int64_t start = System::currentTimeMillis();
                if (pmt == testPmt[0]) {
                    ArithLessBatchOperator(&secretsA, &secretsB, width, System::nextTask(), 0, -1).execute();
                } else if (pmt == testPmt[1]) {
                    auto tempZ = ArithLessBatchOperator(&secretsB, &secretsA, width, System::nextTask(), 0, 1).execute()
                            ->_zis;
                    for (auto &t: tempZ) {
                        t = t ^ Comm::rank();
                    }
                } else if (pmt == testPmt[2]) {
                    ArithBatchOperator(&secretsA, &secretsB, width, System::nextTask(), 0, 0).execute();
                } else if (pmt == testPmt[3]) {
                    ArithLessBatchOperator(&secretsA, &secretsB, width, System::nextTask(), 0, -2).execute();
                } else if (pmt == testPmt[4]) {
                    std::vector<ArithSecret> arithSecrets;
                    arithSecrets.reserve(secretsA.size());
                    for (size_t i = 0; i < secretsA.size(); i++) {
                        arithSecrets.emplace_back(secretsA[i], width, 0);
                    }
                    start = System::currentTimeMillis();
                    Secrets::sort(arithSecrets, true, System::nextTask());
                }
                int64_t arithTime = System::currentTimeMillis() - start;

                // bool compare less than
                boolShare(width, originsA, originsB, secretsA, secretsB);
                start = System::currentTimeMillis();
                BoolLessBatchOperator(&secretsA, &secretsB, width, System::nextTask(), 0, -1).execute();
                int64_t boolTime = System::currentTimeMillis() - start;

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
                    result.num = num;
                    result.width = width;
                    result.avgArithTime = avgArithTime;
                    result.avgBoolTime = avgBoolTime;
                    result.timestamp = timestamp;
                    results.push_back(result);

                    Log::i("Server0 - ArithTime: {}ms, BoolTime: {}ms", arithTime0, boolTime0);
                    Log::i("Server1 - ArithTime: {}ms, BoolTime: {}ms", arithTime1, boolTime1);
                    Log::i("Average - ArithTime: {:.2f}ms, BoolTime: {:.2f}ms", avgArithTime, avgBoolTime);
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
                    "Timestamp,NumElements,Width,AvgArithTime_ms,AvgBoolTime_ms,ArithVsBoolRatio,BoolVsArithSpeedup\n";

            // Write data rows
            for (const auto &result: results) {
                double ratio = result.avgArithTime / result.avgBoolTime;
                double speedup = result.avgBoolTime / result.avgArithTime;

                csvFile << result.timestamp << ","
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
