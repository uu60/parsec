#include "basis/View.h"
#include "basis/Views.h"
#include "compute/batch/bool/BoolAndBatchOperator.h"
#include "compute/batch/bool/BoolEqualBatchOperator.h"
#include "compute/batch/bool/BoolLessBatchOperator.h"
#include "compute/batch/bool/BoolMutexBatchOperator.h"
#include "conf/AppConfig.h"
#include "conf/DbConf.h"
#include "parallel/ThreadPoolSupport.h"
#include "secret/Secrets.h"
#include "utils/Log.h"
#include "utils/Math.h"
#include "utils/System.h"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <future>
#include <numeric>
#include <string>
#include <vector>

using namespace parsec;

namespace {
struct BenchConfig {
    int rows = 100;
    int tableNum = 2;
    int width = 64;
    int rounds = 3;
    int warmup = 1;
    bool hash = false;
};

std::vector<int64_t> oldEqual(std::vector<int64_t> *xs,
                              std::vector<int64_t> *ys,
                              int width,
                              int taskTag,
                              int msgTagOffset) {
    auto lessXy = BoolLessBatchOperator(xs, ys, width, taskTag, msgTagOffset, SecureOperator::NO_CLIENT_COMPUTE)
            .execute()
            ->_zis;
    auto lessYx = BoolLessBatchOperator(ys, xs, width, taskTag,
                                        msgTagOffset + BoolLessBatchOperator::tagStride(),
                                        SecureOperator::NO_CLIENT_COMPUTE)
            .execute()
            ->_zis;

    for (auto &v: lessXy) {
        v ^= Comm::rank();
    }
    for (auto &v: lessYx) {
        v ^= Comm::rank();
    }

    return BoolAndBatchOperator(&lessXy, &lessYx, 1, taskTag,
                                msgTagOffset + 2 * BoolLessBatchOperator::tagStride(),
                                SecureOperator::NO_CLIENT_COMPUTE)
            .execute()
            ->_zis;
}

int oldEqualTagStride() {
    return 2 * BoolLessBatchOperator::tagStride() + BoolAndBatchOperator::tagStride();
}

View nestedLoopJoinOldEqual(View &v0, View &v1, std::string &field0, std::string &field1, bool compress) {
    const size_t effectiveFieldNum0 = v0.colNum() - 2;
    const size_t effectiveFieldNum1 = v1.colNum() - 2;

    std::vector<std::string> fieldNames(effectiveFieldNum0 + effectiveFieldNum1);
    std::string tableName0 = v0._tableName.empty() ? "$t0" : v0._tableName;
    std::string tableName1 = v1._tableName.empty() ? "$t1" : v1._tableName;

    const std::string prefix = Table::BUCKET_TAG_PREFIX;
    auto decorate = [&](const std::string &tableName, const std::string &raw) -> std::string {
        if (raw.compare(0, prefix.size(), prefix) == 0) {
            return prefix + tableName + "." + raw.substr(prefix.size());
        }
        return tableName + "." + raw;
    };

    for (size_t i = 0; i < effectiveFieldNum0; ++i) {
        fieldNames[i] = decorate(tableName0, v0._fieldNames[i]);
    }
    for (size_t i = 0; i < effectiveFieldNum1; ++i) {
        fieldNames[i + effectiveFieldNum0] = decorate(tableName1, v1._fieldNames[i]);
    }

    std::vector<int> fieldWidths(effectiveFieldNum0 + effectiveFieldNum1);
    for (size_t i = 0; i < effectiveFieldNum0; ++i) {
        fieldWidths[i] = v0._fieldWidths[i];
    }
    for (size_t i = 0; i < effectiveFieldNum1; ++i) {
        fieldWidths[i + effectiveFieldNum0] = v1._fieldWidths[i];
    }

    View joined(fieldNames, fieldWidths);
    if (v0._dataCols.empty() || v1._dataCols.empty()) {
        return joined;
    }

    const size_t rows0 = v0.rowNum();
    const size_t rows1 = v1.rowNum();
    if (rows0 == 0 || rows1 == 0) {
        return joined;
    }

    const size_t n = rows0 * rows1;
    for (int i = 0; i < joined.colNum(); ++i) {
        joined._dataCols[i].resize(n);
    }

    const int colIndex0 = v0.colIndex(field0);
    const int colIndex1 = v1.colIndex(field1);
    if (colIndex0 == -1 || colIndex1 == -1) {
        return joined;
    }

    auto &col0 = v0._dataCols[colIndex0];
    auto &col1 = v1._dataCols[colIndex1];
    const int keyWidth = v0._fieldWidths[colIndex0];

    const bool baseline = DbConf::BASELINE_MODE;
    const bool precise = (!DbConf::BASELINE_MODE) && (!DbConf::DISABLE_PRECISE_COMPACTION);
    const int validIdx0 = v0.colNum() + View::VALID_COL_OFFSET;
    const int validIdx1 = v1.colNum() + View::VALID_COL_OFFSET;
    const auto &lvalid = v0._dataCols[validIdx0];
    const auto &rvalid = v1._dataCols[validIdx1];
    const int batchSize = Conf::BATCH_SIZE;

    if (DbConf::BASELINE_MODE || batchSize <= 0 || Conf::DISABLE_MULTI_THREAD) {
        size_t rowIndex = 0;
        std::vector<int64_t> cmp0;
        std::vector<int64_t> cmp1;
        cmp0.reserve(n);
        cmp1.reserve(n);
        std::vector<int64_t> bigLValid;
        std::vector<int64_t> bigRValid;
        if (!precise) {
            bigLValid.resize(n);
            bigRValid.resize(n);
        }

        for (size_t i = 0; i < rows0; ++i) {
            for (size_t j = 0; j < rows1; ++j) {
                for (size_t k = 0; k < effectiveFieldNum0; ++k) {
                    joined._dataCols[k][rowIndex] = v0._dataCols[k][i];
                }
                for (size_t k = 0; k < effectiveFieldNum1; ++k) {
                    joined._dataCols[k + effectiveFieldNum0][rowIndex] = v1._dataCols[k][j];
                }
                cmp0.push_back(col0[i]);
                cmp1.push_back(col1[j]);
                if (!precise) {
                    bigLValid[rowIndex] = lvalid[i];
                    bigRValid[rowIndex] = rvalid[j];
                }
                ++rowIndex;
            }
        }

        auto eqRes = oldEqual(&cmp0, &cmp1, keyWidth, 0, 0);
        if (precise) {
            joined._dataCols[joined.colNum() + View::VALID_COL_OFFSET] = std::move(eqRes);
        } else {
            auto pairValid = BoolAndBatchOperator(&bigLValid, &bigRValid, 1,
                                                  0, oldEqualTagStride(),
                                                  SecureOperator::NO_CLIENT_COMPUTE)
                    .execute()
                    ->_zis;
            joined._dataCols[joined.colNum() + View::VALID_COL_OFFSET] =
                    BoolAndBatchOperator(&eqRes, &pairValid, 1,
                                         0, oldEqualTagStride() + BoolAndBatchOperator::tagStride(),
                                         SecureOperator::NO_CLIENT_COMPUTE)
                            .execute()
                            ->_zis;
        }
    } else {
        const int eqStride = oldEqualTagStride();
        const int andStride = BoolAndBatchOperator::tagStride();
        const int blockStride = eqStride + 2 * andStride;
        const int batchNum = static_cast<int>((n + batchSize - 1) / batchSize);
        std::vector<std::future<std::vector<int64_t> > > futures(batchNum);

        for (int b = 0; b < batchNum; ++b) {
            futures[b] = ThreadPoolSupport::submit([&, b]() {
                const size_t start = static_cast<size_t>(b) * batchSize;
                const size_t end = std::min(n, start + batchSize);
                const size_t cnt = end - start;
                std::vector<int64_t> cmp0(cnt), cmp1(cnt);
                std::vector<int64_t> lval, rval;
                if (!precise) {
                    lval.resize(cnt);
                    rval.resize(cnt);
                }

                for (size_t off = 0; off < cnt; ++off) {
                    const size_t g = start + off;
                    const size_t i = g / rows1;
                    const size_t j = g % rows1;
                    cmp0[off] = col0[i];
                    cmp1[off] = col1[j];
                    if (!precise) {
                        lval[off] = lvalid[i];
                        rval[off] = rvalid[j];
                    }
                }

                const int baseTag = b * blockStride;
                auto eqRes = oldEqual(&cmp0, &cmp1, keyWidth, 0, baseTag);
                if (precise) {
                    return eqRes;
                }

                auto pairValid = BoolAndBatchOperator(&lval, &rval, 1,
                                                      0, baseTag + eqStride,
                                                      SecureOperator::NO_CLIENT_COMPUTE)
                        .execute()
                        ->_zis;
                return BoolAndBatchOperator(&eqRes, &pairValid, 1,
                                            0, baseTag + eqStride + andStride,
                                            SecureOperator::NO_CLIENT_COMPUTE)
                        .execute()
                        ->_zis;
            });
        }

        size_t rowIndex = 0;
        for (size_t i = 0; i < rows0; ++i) {
            for (size_t j = 0; j < rows1; ++j) {
                for (size_t k = 0; k < effectiveFieldNum0; ++k) {
                    joined._dataCols[k][rowIndex] = v0._dataCols[k][i];
                }
                for (size_t k = 0; k < effectiveFieldNum1; ++k) {
                    joined._dataCols[k + effectiveFieldNum0][rowIndex] = v1._dataCols[k][j];
                }
                ++rowIndex;
            }
        }

        rowIndex = 0;
        for (auto &future: futures) {
            auto result = future.get();
            for (auto bit: result) {
                joined._dataCols[joined.colNum() + View::VALID_COL_OFFSET][rowIndex++] = bit;
            }
        }
    }

    if (compress && !baseline) {
        joined.clearInvalidEntries(0);
    }
    return joined;
}

std::vector<std::vector<std::vector<int64_t> > > butterflyPermutationLocal(View &view,
                                                                            int tagColIndex,
                                                                            int msgTagBase) {
    int numBuckets = DbConf::SHUFFLE_BUCKET_NUM;
    int numLayers = static_cast<int>(std::log2(numBuckets));
    std::vector<std::vector<std::vector<int64_t> > > buckets(numBuckets);
    int rowNum = view.rowNum();

    for (int b = 0; b < numBuckets; b++) {
        buckets[b].reserve(view.colNum() - 1);
        for (int col = 0; col < view.colNum() - 1; col++) {
            buckets[b].push_back(std::vector<int64_t>());
        }
    }

    for (int col = 0; col < view.colNum() - 1; col++) {
        for (int row = 0; row < rowNum; row++) {
            buckets[row % numBuckets][col].push_back(view._dataCols[col][row]);
        }
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
        mergedDatas.reserve(view.colNum() * view.rowNum() * reserved);
        std::vector<int64_t> dummyDatas;
        std::vector<int64_t> routingBits;
        routingBits.reserve(view.colNum() * view.rowNum() * reserved);

        for (int i = 0; i < numBuckets; i += stride * 2) {
            for (int j = 0; j < stride; j++) {
                int idx1 = i + j;
                int idx2 = i + j + stride;
                if (idx2 >= numBuckets) {
                    continue;
                }

                auto &bucket1 = buckets[idx1];
                auto &bucket2 = buckets[idx2];
                int bitPosition = numLayers - layer - 1;
                size_t rows1 = bucket1.empty() ? 0 : bucket1[0].size();
                size_t rows2 = bucket2.empty() ? 0 : bucket2[0].size();
                size_t totalRows = rows1 + rows2;
                if (totalRows == 0) {
                    continue;
                }

                totalRowsV.push_back(static_cast<int>(totalRows));
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

        std::vector<int64_t> allResults;
        if (Conf::DISABLE_MULTI_THREAD || Conf::BATCH_SIZE <= 0 || mergedDatas.size() <= Conf::BATCH_SIZE) {
            allResults = BoolMutexBatchOperator(&mergedDatas, &dummyDatas, &routingBits,
                                                view._maxWidth, 0, msgTagBase)
                    .execute()
                    ->_zis;
        } else {
            const size_t totalCnt = mergedDatas.size();
            const int batchSize = Conf::BATCH_SIZE;
            const int numBatches = static_cast<int>((totalCnt + batchSize - 1) / batchSize);
            allResults.resize(totalCnt * 2);
            const int tagStride = BoolMutexBatchOperator::tagStride();
            std::vector<std::future<std::vector<int64_t> > > futures;
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

                    return BoolMutexBatchOperator(&subMerged, &subDummy, &subRouting,
                                                  view._maxWidth, 0, msgTagBase + tagStride * b)
                            .execute()
                            ->_zis;
                }));
            }

            size_t frontPos = 0;
            size_t backPos = totalCnt;
            for (auto &f: futures) {
                auto subResult = f.get();
                int subHalf = static_cast<int>(subResult.size() / 2);
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
            auto &bucket1 = buckets[indices1[i]];
            auto &bucket2 = buckets[indices2[i]];
            std::vector<std::vector<int64_t> > newBucket1;
            std::vector<std::vector<int64_t> > newBucket2;

            for (int col = 0; col < bucket1.size(); col++) {
                auto data1Start = allResults.begin() + resultIndex;
                auto data1End = data1Start + totalRowsV[i];
                auto data2Start = data1Start + half;
                auto data2End = data2Start + totalRowsV[i];
                newBucket1.emplace_back(data1Start, data1End);
                newBucket2.emplace_back(data2Start, data2End);
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

View performBucketJoinsLocal(std::vector<std::vector<std::vector<int64_t> > > &buckets0,
                             std::vector<std::vector<std::vector<int64_t> > > &buckets1,
                             View &v0,
                             View &v1,
                             std::string &field0,
                             std::string &field1,
                             bool compress,
                             bool useOldEqual) {
    bool firstResult = true;
    View result;

    for (size_t b = 0; b < buckets0.size(); ++b) {
        if (buckets0[b].empty() || buckets1[b].empty() ||
            buckets0[b][0].empty() || buckets1[b][0].empty()) {
            continue;
        }

        View left(v0._tableName, v0._fieldNames, v0._fieldWidths, false);
        left._dataCols = buckets0[b];
        left._dataCols.emplace_back(left.rowNum(), 0);
        View right(v1._tableName, v1._fieldNames, v1._fieldWidths, false);
        right._dataCols = buckets1[b];
        right._dataCols.emplace_back(right.rowNum(), 0);

        View joined = useOldEqual
                              ? nestedLoopJoinOldEqual(left, right, field0, field1, compress)
                              : Views::nestedLoopJoin(left, right, field0, field1, compress);
        if (joined._dataCols.empty() || joined._dataCols[0].empty()) {
            continue;
        }

        if (firstResult) {
            result = joined;
            firstResult = false;
        } else {
            for (size_t col = 0; col < result._dataCols.size(); ++col) {
                result._dataCols[col].insert(result._dataCols[col].end(),
                                             joined._dataCols[col].begin(),
                                             joined._dataCols[col].end());
            }
        }
    }

    if (firstResult) {
        std::vector<std::string> fieldNames;
        std::vector<int> fieldWidths;
        size_t eff0 = v0.colNum() - 2;
        size_t eff1 = v1.colNum() - 2;
        fieldNames.reserve(eff0 + eff1);
        fieldWidths.reserve(eff0 + eff1);
        std::string t0 = v0._tableName.empty() ? "$t0" : v0._tableName;
        std::string t1 = v1._tableName.empty() ? "$t1" : v1._tableName;
        for (size_t i = 0; i < eff0; ++i) {
            fieldNames.emplace_back(t0 + "." + v0._fieldNames[i]);
            fieldWidths.emplace_back(v0._fieldWidths[i]);
        }
        for (size_t i = 0; i < eff1; ++i) {
            fieldNames.emplace_back(t1 + "." + v1._fieldNames[i]);
            fieldWidths.emplace_back(v1._fieldWidths[i]);
        }
        result = View(fieldNames, fieldWidths);
    }

    return result;
}

View hashJoinLocal(View &v0, View &v1, std::string &field0, std::string &field1, bool compress, bool useOldEqual) {
    if (DbConf::BASELINE_MODE) {
        return useOldEqual
                       ? nestedLoopJoinOldEqual(v0, v1, field0, field1, compress)
                       : Views::nestedLoopJoin(v0, v1, field0, field1, compress);
    }

    int tagColIndex0 = -1;
    int tagColIndex1 = -1;
    for (int i = 0; i < v0._fieldNames.size(); i++) {
        if (v0._fieldNames[i].find(Table::BUCKET_TAG_PREFIX) == 0) {
            tagColIndex0 = i;
            break;
        }
    }
    for (int i = 0; i < v1._fieldNames.size(); i++) {
        if (v1._fieldNames[i].find(Table::BUCKET_TAG_PREFIX) == 0) {
            tagColIndex1 = i;
            break;
        }
    }

    if (tagColIndex0 == -1 || tagColIndex1 == -1) {
        return useOldEqual
                       ? nestedLoopJoinOldEqual(v0, v1, field0, field1, compress)
                       : Views::nestedLoopJoin(v0, v1, field0, field1, compress);
    }

    auto buckets0 = butterflyPermutationLocal(v0, tagColIndex0, 0);
    auto buckets1 = butterflyPermutationLocal(v1, tagColIndex1, 0);
    return performBucketJoinsLocal(buckets0, buckets1, v0, v1, field0, field1, compress, useOldEqual);
}

void normalizeJoinResult(View &joinResult) {
    if (joinResult._fieldNames.empty()) {
        return;
    }

    std::string oldTagName = Table::BUCKET_TAG_PREFIX + joinResult._fieldNames[0];
    std::string newTagName = Table::BUCKET_TAG_PREFIX + "id";
    for (size_t j = 0; j < joinResult._fieldNames.size(); j++) {
        if (joinResult._fieldNames[j] == oldTagName) {
            joinResult._fieldNames[j] = newTagName;
            break;
        }
    }

    joinResult._fieldNames[0] = "id";
    std::vector<std::string> fieldNames = {"id", "$tag:id"};
    joinResult.select(fieldNames);
}

std::vector<View> buildViews(const std::vector<std::vector<int64_t> > &idShares,
                             const std::vector<std::vector<int64_t> > &tagShares,
                             const BenchConfig &cfg) {
    std::vector<Table> tables;
    std::vector<View> views;
    tables.reserve(cfg.tableNum);
    views.reserve(cfg.tableNum);

    for (int tableIdx = 0; tableIdx < cfg.tableNum; ++tableIdx) {
        std::vector<std::string> fieldNames = {"id"};
        std::vector<int> fieldWidths = {cfg.width};
        std::string tableName = "table" + std::to_string(tableIdx);
        std::string keyField = "id";
        Table table(tableName, fieldNames, fieldWidths, keyField);

        for (int row = 0; row < cfg.rows; ++row) {
            table.insert({idShares[tableIdx][row], tagShares[tableIdx][row]});
        }

        tables.push_back(std::move(table));
        views.push_back(Views::selectAll(tables.back()));
    }

    return views;
}

View runJoin(std::vector<View> views, const BenchConfig &cfg, bool useOldEqual) {
    View joinResult = views[0];
    std::string joinField = "id";

    for (int i = 1; i < cfg.tableNum; ++i) {
        if (cfg.hash) {
            joinResult = hashJoinLocal(joinResult, views[i], joinField, joinField, false, useOldEqual);
        } else if (useOldEqual) {
            joinResult = nestedLoopJoinOldEqual(joinResult, views[i], joinField, joinField, false);
        } else {
            joinResult = Views::nestedLoopJoin(joinResult, views[i], joinField, joinField, false);
        }
        normalizeJoinResult(joinResult);
    }

    return joinResult;
}

double average(const std::vector<int64_t> &values) {
    if (values.empty()) {
        return 0.0;
    }
    auto sum = std::accumulate(values.begin(), values.end(), int64_t{0});
    return static_cast<double>(sum) / static_cast<double>(values.size());
}

void printResult(const std::string &name, const BenchConfig &cfg, const std::vector<int64_t> &times, size_t rows) {
    if (!Comm::isServer() || Comm::rank() != 0) {
        return;
    }
    auto sorted = times;
    std::sort(sorted.begin(), sorted.end());
    auto median = sorted.empty() ? 0 : sorted[sorted.size() / 2];
    auto min = sorted.empty() ? 0 : sorted.front();
    auto max = sorted.empty() ? 0 : sorted.back();
    Log::i("DB_JOIN_COMPARE impl={} hash={} rows={} table_num={} width={} final_rows={} rounds={} avg_ms={} median_ms={} min_ms={} max_ms={}",
           name, cfg.hash, cfg.rows, cfg.tableNum, cfg.width, rows, cfg.rounds, average(times), median, min, max);
}
}

int main(int argc, char *argv[]) {
    System::init(argc, argv);
    DbConf::init();

    BenchConfig cfg;
    cfg.rows = AppConfig::getInt("rows", cfg.rows);
    cfg.tableNum = AppConfig::getInt("table_num", cfg.tableNum);
    cfg.width = AppConfig::getInt("width", cfg.width);
    cfg.rounds = AppConfig::getInt("rounds", cfg.rounds);
    cfg.warmup = AppConfig::getInt("warmup", cfg.warmup);
    cfg.hash = AppConfig::getBool("hash", cfg.hash);

    if (cfg.rows <= 0) {
        throw std::runtime_error("rows must be positive.");
    }
    if (cfg.tableNum < 2) {
        throw std::runtime_error("table_num must be at least 2.");
    }
    if (cfg.width < 1 || cfg.width > 64) {
        throw std::runtime_error("width must be in [1, 64].");
    }
    std::vector<std::vector<int64_t> > plainIds(cfg.tableNum);
    std::vector<std::vector<int64_t> > plainTags(cfg.tableNum);
    int64_t mask = cfg.width >= 63 ? -1LL : ((1LL << cfg.width) - 1);

    for (int tableIdx = 0; tableIdx < cfg.tableNum; ++tableIdx) {
        plainIds[tableIdx].resize(cfg.rows);
        plainTags[tableIdx].resize(cfg.rows);
        if (Comm::isClient()) {
            for (int row = 0; row < cfg.rows; ++row) {
                plainIds[tableIdx][row] = Math::randInt() & mask;
                if (tableIdx > 0 && row % 7 == 0) {
                    plainIds[tableIdx][row] = plainIds[0][row];
                }
                plainTags[tableIdx][row] = (plainIds[tableIdx][row] * 31 + 17) % DbConf::SHUFFLE_BUCKET_NUM;
            }
        }
    }

    std::vector<std::vector<int64_t> > idShares(cfg.tableNum);
    std::vector<std::vector<int64_t> > tagShares(cfg.tableNum);
    for (int tableIdx = 0; tableIdx < cfg.tableNum; ++tableIdx) {
        idShares[tableIdx] = Secrets::boolShare(plainIds[tableIdx], 2, cfg.width, 0);
        tagShares[tableIdx] = Secrets::boolShare(plainTags[tableIdx], 2, cfg.width, 0);
    }

    std::vector<int64_t> oldTimes;
    std::vector<int64_t> newTimes;
    oldTimes.reserve(cfg.rounds);
    newTimes.reserve(cfg.rounds);
    size_t oldRows = 0;
    size_t newRows = 0;

    if (Comm::isServer()) {
        auto views = buildViews(idShares, tagShares, cfg);
        for (int iter = 0; iter < cfg.warmup + cfg.rounds; ++iter) {
            auto start = System::currentTimeMillis();
            auto result = runJoin(views, cfg, true);
            auto elapsed = System::currentTimeMillis() - start;
            if (iter >= cfg.warmup) {
                oldTimes.push_back(elapsed);
            }
            oldRows = result._dataCols.empty() ? 0 : result._dataCols[0].size();
        }

        for (int iter = 0; iter < cfg.warmup + cfg.rounds; ++iter) {
            auto start = System::currentTimeMillis();
            auto result = runJoin(views, cfg, false);
            auto elapsed = System::currentTimeMillis() - start;
            if (iter >= cfg.warmup) {
                newTimes.push_back(elapsed);
            }
            newRows = result._dataCols.empty() ? 0 : result._dataCols[0].size();
        }
    }

    printResult("old_less_based_join", cfg, oldTimes, oldRows);
    printResult("new_current_join", cfg, newTimes, newRows);

    System::finalize();
    return 0;
}
