#include "secret/Secrets.h"
#include "utils/System.h"
#include "utils/Log.h"
#include "utils/Math.h"
#include "conf/DbConf.h"
#include "comm/Comm.h"
#include "parallel/ThreadPoolSupport.h"

#include "compute/batch/bool/BoolMutexBatchOperator.h"

#include "../include/basis/View.h"
#include "../include/basis/Table.h"

#include <string>
#include <vector>
#include <cmath>
#include <future>

std::vector<std::vector<std::vector<int64_t>>> butterflyPermutation(
    View &view,
    int tagColIndex,
    int msgTagBase
) {
    int numBuckets = DbConf::SHUFFLE_BUCKET_NUM;
    int numLayers = static_cast<int>(std::log2(numBuckets));

    std::vector<std::vector<std::vector<int64_t>>> buckets(numBuckets);
    buckets[0].reserve(view.colNum() - 1);

    for (int col = 0; col < view.colNum() - 1; col++) {
        buckets[0].push_back(view._dataCols[col]);
    }

    int reserved = numBuckets / 2;
    for (int layer = 0; layer < numLayers; layer++) {
        int stride = 1 << layer;

        std::vector<int> totalRowsV;
        std::vector<int> indices1;
        std::vector<int> indices2;
        totalRowsV.reserve(reserved);
        indices1.reserve(reserved * view.rowNum());
        indices2.reserve(reserved * view.rowNum());

        std::vector<int64_t> mergedDatas;
        mergedDatas.reserve((view.colNum() * view.rowNum() * reserved));

        std::vector<int64_t> dummyDatas;

        std::vector<int64_t> routingBits;
        routingBits.reserve((view.colNum() * view.rowNum() * reserved));

        for (int i = 0; i < numBuckets; i += stride * 2) {
            for (int j = 0; j < stride; j++) {
                int idx1 = i + j;
                int idx2 = i + j + stride;
                std::vector<std::vector<int64_t>> &bucket1 = buckets[idx1];
                std::vector<std::vector<int64_t>> &bucket2 = buckets[idx2];

                if (idx2 < numBuckets) {
                    int bitPosition = numLayers - layer - 1;
                    size_t rows1 = bucket1.empty() ? 0 : bucket1[0].size();
                    size_t rows2 = bucket2.empty() ? 0 : bucket2[0].size();
                    size_t totalRows = rows1 + rows2;

                    if (totalRows == 0) {
                        continue;
                    }

                    totalRowsV.push_back(totalRows);
                    indices1.push_back(idx1);
                    indices2.push_back(idx2);

                    for (int col = 0; col < bucket1.size(); col++) {
                        for (size_t r = 0; r < rows1; r++) {
                            routingBits.push_back(Math::getBit(bucket1[tagColIndex][r], bitPosition));
                        }

                        for (size_t r = 0; r < rows2; r++) {
                            routingBits.push_back(Math::getBit(bucket2[tagColIndex][r], bitPosition));
                        }

                        if (!bucket1.empty()) {
                            mergedDatas.insert(mergedDatas.end(), bucket1[col].begin(), bucket1[col].end());
                        }
                        if (!bucket2.empty()) {
                            mergedDatas.insert(mergedDatas.end(), bucket2[col].begin(), bucket2[col].end());
                        }

                        dummyDatas.resize(mergedDatas.size(), 0);
                    }
                }
            }
        }

        std::vector<int64_t> allResults;
        if (Conf::DISABLE_MULTI_THREAD || Conf::BATCH_SIZE <= 0 || mergedDatas.size() <= Conf::BATCH_SIZE) {
            allResults = BoolMutexBatchOperator(
                &mergedDatas, &dummyDatas, &routingBits, view._maxWidth, 0,
                msgTagBase).execute()->_zis;
        } else {
            const size_t totalCnt = mergedDatas.size();
            const int batchSize = Conf::BATCH_SIZE;
            const int numBatches = static_cast<int>((totalCnt + batchSize - 1) / batchSize);

            allResults.resize(totalCnt * 2);

            const int tagStride = BoolMutexBatchOperator::tagStride();

            std::vector<std::future<std::vector<int64_t>>> futures;
            futures.reserve(numBatches);

            for (int b = 0; b < numBatches; ++b) {
                futures.emplace_back(ThreadPoolSupport::submit([&, b] {
                    const size_t start = static_cast<size_t>(b) * batchSize;
                    const size_t end = std::min(totalCnt, start + batchSize);
                    const size_t cnt = end - start;

                    std::vector<int64_t> subMerged(cnt);
                    std::vector<int64_t> subDummy(cnt);
                    std::vector<int64_t> subRouting(cnt);
                    std::copy_n(mergedDatas.begin() + start, cnt, subMerged.begin());
                    std::copy_n(dummyDatas.begin() + start, cnt, subDummy.begin());
                    std::copy_n(routingBits.begin() + start, cnt, subRouting.begin());

                    auto subResults = BoolMutexBatchOperator(
                        &subMerged, &subDummy, &subRouting,
                        view._maxWidth, 0,
                        msgTagBase + tagStride * b
                    ).execute()->_zis;

                    return subResults;
                }));
            }

            size_t frontPos = 0;
            size_t backPos = totalCnt;
            for (auto &f: futures) {
                auto subResult = f.get();
                int subHalf = subResult.size() / 2;
                std::copy_n(subResult.begin(), subHalf, allResults.begin() + frontPos);
                std::copy_n(subResult.begin() + subHalf, subHalf, allResults.begin() + backPos);

                frontPos += subHalf;
                backPos += subHalf;
            }
        }

        int resultIndex = 0;
        int half = static_cast<int>(allResults.size() / 2);
        for (int i = 0; i < indices1.size(); i++) {
            if (totalRowsV[i] == 0) {
                continue;
            }
            std::vector<std::vector<int64_t>> &bucket1 = buckets[indices1[i]];
            std::vector<std::vector<int64_t>> &bucket2 = buckets[indices2[i]];
            std::vector<std::vector<int64_t>> newBucket1;
            std::vector<std::vector<int64_t>> newBucket2;

            for (int col = 0; col < bucket1.size(); col++) {
                auto data1Start = allResults.begin() + resultIndex;
                auto data1End = data1Start + totalRowsV[i];
                auto data2Start = data1Start + half;
                auto data2End = data2Start + totalRowsV[i];
                auto bucket1Data = std::vector(data1Start, data1End);
                auto bucket2Data = std::vector(data2Start, data2End);

                newBucket1.push_back(std::move(bucket1Data));
                newBucket2.push_back(std::move(bucket2Data));
                resultIndex += totalRowsV[i];
            }

            View temp1(view._tableName, view._fieldNames, view._fieldWidths, false);
            View temp2(view._tableName, view._fieldNames, view._fieldWidths, false);
            temp1._dataCols = std::move(newBucket1);
            temp1._dataCols.emplace_back(temp1.rowNum(), 0);
            temp2._dataCols = std::move(newBucket2);
            temp2._dataCols.emplace_back(temp2.rowNum(), 0);
            temp1.clearInvalidEntries(msgTagBase);
            temp2.clearInvalidEntries(msgTagBase);

            newBucket1 = std::move(temp1._dataCols);
            newBucket2 = std::move(temp2._dataCols);
            newBucket1.resize(newBucket1.size() - 1);
            newBucket2.resize(newBucket2.size() - 1);

            bucket1 = std::move(newBucket1);
            bucket2 = std::move(newBucket2);
        }
    }

    return buckets;
}

int main(int argc, char *argv[]) {
    System::init(argc, argv);
    DbConf::init();

    int rows = 100;
    int numBuckets = DbConf::SHUFFLE_BUCKET_NUM;
    
    if (Conf::_userParams.count("rows")) {
        rows = std::stoi(Conf::_userParams["rows"]);
    }

    if (Conf::_userParams.count("buckets")) {
        int customBuckets = std::stoi(Conf::_userParams["buckets"]);
        if ((customBuckets & (customBuckets - 1)) == 0 && customBuckets > 0) {
            DbConf::SHUFFLE_BUCKET_NUM = customBuckets;
            numBuckets = customBuckets;
        } else {
            Log::e("Bucket number must be a power of 2, using default: {}", numBuckets);
        }
    }

    Log::ir(2, "========================================");
    Log::ir(2, "Shuffle Performance Benchmark");
    Log::ir(2, "========================================");
    Log::ir(2, "Rows: {}", rows);
    Log::ir(2, "Buckets: {}", numBuckets);
    Log::ir(2, "Batch Size: {}", Conf::BATCH_SIZE);
    Log::ir(2, "Multi-thread: {}", !Conf::DISABLE_MULTI_THREAD);
    Log::ir(2, "========================================");

    std::vector<int64_t> dataShares(rows);
    std::vector<int64_t> tagShares(rows);

    if (Comm::rank() == 2) {
        for (int i = 0; i < rows; i++) {
            dataShares[i] = Math::randInt();
            int64_t keyValue = dataShares[i];
            tagShares[i] = (keyValue * 31 + 17) % numBuckets;
        }
    }

    dataShares = Secrets::boolShare(dataShares, 2, 64, 0);
    tagShares = Secrets::boolShare(tagShares, 2, 64, 0);

    if (Comm::isServer()) {
        std::vector<std::string> fieldNames = {"data", "$tag:data"};
        std::vector<int> fieldWidths = {64, 64};
        std::string tableName = "test_table";

        View testView(tableName, fieldNames, fieldWidths);
        testView._dataCols[0] = dataShares;
        testView._dataCols[1] = tagShares;
    testView._dataCols[2] = std::vector<int64_t>(rows, Comm::rank());
    testView._dataCols[3] = std::vector<int64_t>(rows, 0);

    int tagColIndex = 1;

        Log::ir(2, "Starting butterfly permutation shuffle...");
        auto startTime = System::currentTimeMillis();
        auto buckets = butterflyPermutation(testView, tagColIndex, 0);

        auto elapsedTime = System::currentTimeMillis() - startTime;

        Log::ir(0, "========================================");
        Log::ir(0, "Shuffle completed in {} ms", elapsedTime);
        Log::ir(0, "========================================");

        Log::ir(0, "Bucket distribution:");
        int totalRowsInBuckets = 0;
        for (int i = 0; i < buckets.size(); i++) {
            int bucketRows = buckets[i].empty() || buckets[i][0].empty() ? 0 : buckets[i][0].size();
            totalRowsInBuckets += bucketRows;
            if (bucketRows > 0) {
                Log::ir(0, "  Bucket {}: {} rows", i, bucketRows);
            }
        }
        Log::ir(0, "Total rows in buckets: {}", totalRowsInBuckets);
        Log::ir(0, "Original rows: {}", rows);
        Log::ir(0, "========================================");

        double rowsPerMs = static_cast<double>(rows) / elapsedTime;
    double throughput = rowsPerMs * 1000.0;
        
        Log::ir(2, "Performance Metrics:");
        Log::ir(2, "  Throughput: {:.2f} rows/second", throughput);
        Log::ir(2, "  Latency: {:.4f} ms/row", static_cast<double>(elapsedTime) / rows);
        Log::ir(2, "========================================");
    }

    System::finalize();
    return 0;
}
