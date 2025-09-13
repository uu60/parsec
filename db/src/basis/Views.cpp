//
// Created by 杜建璋 on 25-6-2.
//

#include "../../include/basis/Views.h"

#include "compute/batch/bool/BoolEqualBatchOperator.h"
#include "compute/batch/bool/BoolXorBatchOperator.h"
#include "compute/batch/bool/BoolMutexBatchOperator.h"
#include "compute/batch/bool/BoolToArithBatchOperator.h"
#include "compute/batch/arith/ArithAddBatchOperator.h"
#include "compute/batch/arith/ArithLessBatchOperator.h"
#include "conf/DbConf.h"
#include "parallel/ThreadPoolSupport.h"
#include "utils/Math.h"
#include "comm/Comm.h"
#include "secret/Secrets.h"
#include "utils/StringUtils.h"
#include "utils/Log.h"
#include <cmath>
#include <numeric>

#include <string>

#include "compute/batch/bool/BoolAndBatchOperator.h"

View Views::selectAll(Table &t) {
    View v(t._tableName, t._fieldNames, t._fieldWidths);
    v._dataCols = t._dataCols;
    v._dataCols.emplace_back(t._dataCols[0].size(), Comm::rank());
    v._dataCols.emplace_back(t._dataCols[0].size());
    return v;
}

View Views::selectAllWithFieldPrefix(Table &t) {
    std::vector<std::string> fieldNames(t._fieldNames.size());
    for (int i = 0; i < t._fieldNames.size(); i++) {
        fieldNames[i] = getAliasColName(t._tableName, t._fieldNames[i]);
    }

    View v(t._tableName, fieldNames, t._fieldWidths);
    v._dataCols = t._dataCols;
    v._dataCols.emplace_back(t.rowNum(), Comm::rank());
    v._dataCols.emplace_back(t.rowNum());
    return v;
}

View Views::selectColumns(Table &t, std::vector<std::string> &fieldNames) {
    std::vector<size_t> indices;
    indices.reserve(fieldNames.size());
    for (auto &name: fieldNames) {
        auto it = std::find(t._fieldNames.begin(), t._fieldNames.end(), name);
        indices.push_back(std::distance(t._fieldNames.begin(), it));
    }

    std::vector<int> widths;
    widths.reserve(indices.size());
    for (auto idx: indices) {
        widths.push_back(t._fieldWidths[idx]);
    }

    View v(t._tableName, fieldNames, widths);

    if (t._dataCols.empty()) {
        return v;
    }

    int colIndex = 0;
    for (auto idx: indices) {
        v._dataCols[colIndex++] = t._dataCols[idx];
    }

    v._dataCols[v.colNum() + View::PADDING_COL_OFFSET] = std::vector<int64_t>(v.rowNum(), 0);
    v._dataCols[v.colNum() + View::VALID_COL_OFFSET] = std::vector<int64_t>(v.rowNum(), Comm::rank());

    return v;
}

View Views::nestedLoopJoin(View &v0, View &v1, std::string &field0, std::string &field1) {
    // remove padding and valid columns
    const size_t effectiveFieldNum0 = v0.colNum() - 2;
    const size_t effectiveFieldNum1 = v1.colNum() - 2;

    std::vector<std::string> fieldNames(effectiveFieldNum0 + effectiveFieldNum1);
    std::string tableName0 = v0._tableName.empty() ? "$t0" : v0._tableName;
    std::string tableName1 = v1._tableName.empty() ? "$t1" : v1._tableName;

    const std::string prefix = Table::BUCKET_TAG_PREFIX;
    auto decorate = [&](const std::string &tableName, const std::string &raw) -> std::string {
        if (raw.compare(0, prefix.size(), prefix) == 0) {
            return prefix + tableName + "." + raw.substr(prefix.size());
        } else {
            return tableName + "." + raw;
        }
    };

    for (size_t i = 0; i < effectiveFieldNum0; ++i) fieldNames[i] = decorate(tableName0, v0._fieldNames[i]);
    for (size_t i = 0; i < effectiveFieldNum1; ++i) fieldNames[i + effectiveFieldNum0] = decorate(tableName1, v1._fieldNames[i]);

    std::vector<int> fieldWidths(effectiveFieldNum0 + effectiveFieldNum1);
    for (size_t i = 0; i < effectiveFieldNum0; ++i) fieldWidths[i] = v0._fieldWidths[i];
    for (size_t i = 0; i < effectiveFieldNum1; ++i) fieldWidths[i + effectiveFieldNum0] = v1._fieldWidths[i];

    View joined(fieldNames, fieldWidths);
    if (v0._dataCols.empty() || v1._dataCols.empty()) return joined;

    const size_t rows0 = v0.rowNum();
    const size_t rows1 = v1.rowNum();
    if (rows0 == 0 || rows1 == 0) return joined;

    const size_t n = rows0 * rows1;
    for (int i = 0; i < joined.colNum(); ++i) {
        joined._dataCols[i].resize(n);
    }

    const int colIndex0 = v0.colIndex(field0);
    const int colIndex1 = v1.colIndex(field1);
    if (colIndex0 == -1 || colIndex1 == -1) return joined;

    auto &col0 = v0._dataCols[colIndex0];
    auto &col1 = v1._dataCols[colIndex1];
    const int keyWidth = v0._fieldWidths[colIndex0];

    // 三种模式
    const bool BASELINE = DbConf::BASELINE_MODE;
    const bool PRECISE  = (!DbConf::BASELINE_MODE) && (!DbConf::DISABLE_PRECISE_COMPACTION);
    const bool APPROX   = (!DbConf::BASELINE_MODE) && ( DbConf::DISABLE_PRECISE_COMPACTION);

    // 左右 valid（布尔份额）
    const int validIdx0 = v0.colNum() + View::VALID_COL_OFFSET;
    const int validIdx1 = v1.colNum() + View::VALID_COL_OFFSET;
    const auto &lvalid  = v0._dataCols[validIdx0];
    const auto &rvalid  = v1._dataCols[validIdx1];

    const int batchSize = Conf::BATCH_SIZE;

    if (batchSize <= 0 || Conf::DISABLE_MULTI_THREAD) {
        // ---------- 单线程路径 ----------
        size_t rowIndex = 0;
        std::vector<int64_t> cmp0; cmp0.reserve(n);
        std::vector<int64_t> cmp1; cmp1.reserve(n);

        // 为可能的 BASELINE/APPROX 预备成对的 valid 展开
        std::vector<int64_t> bigLValid, bigRValid;
        if (!PRECISE) {
            bigLValid.resize(n);
            bigRValid.resize(n);
        }

        for (size_t i = 0; i < rows0; ++i) {
            for (size_t j = 0; j < rows1; ++j) {
                // 复制数据列
                for (size_t k = 0; k < effectiveFieldNum0; ++k)
                    joined._dataCols[k][rowIndex] = v0._dataCols[k][i];
                for (size_t k = 0; k < effectiveFieldNum1; ++k)
                    joined._dataCols[k + effectiveFieldNum0][rowIndex] = v1._dataCols[k][j];

                cmp0.push_back(col0[i]);
                cmp1.push_back(col1[j]);

                if (!PRECISE) {
                    bigLValid[rowIndex] = lvalid[i];
                    bigRValid[rowIndex] = rvalid[j];
                }
                ++rowIndex;
            }
        }

        // 等值比较
        auto eqRes = BoolEqualBatchOperator(&cmp0, &cmp1, keyWidth,
                                            /*tid*/0, /*msgTag*/0,
                                            SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        // 组合 valid
        std::vector<int64_t> outValid;
        if (PRECISE) {
            outValid = std::move(eqRes);
        } else {
            // pairValid = L ∧ R
            auto pairValid = BoolAndBatchOperator(&bigLValid, &bigRValid, 1,
                                                  /*tid*/0,
                                                  /*msgTag*/BoolEqualBatchOperator::tagStride(),
                                                  SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
            // outValid = eq ∧ pairValid
            outValid = BoolAndBatchOperator(&eqRes, &pairValid, 1,
                                            /*tid*/0,
                                            /*msgTag*/BoolEqualBatchOperator::tagStride() + BoolAndBatchOperator::tagStride(),
                                            SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        }

        joined._dataCols[joined.colNum() + View::VALID_COL_OFFSET] = std::move(outValid);
    } else {
        // ---------- 多线程/分批路径 ----------
        const int eqStride  = BoolEqualBatchOperator::tagStride();
        const int andStride = BoolAndBatchOperator::tagStride();
        const int blockStride = eqStride + 2 * andStride; // 每批占用的 tag 空间

        const int batchNum = static_cast<int>((n + batchSize - 1) / batchSize);
        std::vector<std::future<std::vector<int64_t>>> futures(batchNum);

        for (int b = 0; b < batchNum; ++b) {
            futures[b] = ThreadPoolSupport::submit([&, b]() {
                const size_t start = static_cast<size_t>(b) * batchSize;
                const size_t end   = std::min(n, start + batchSize);
                const size_t cnt   = end - start;

                std::vector<int64_t> cmp0(cnt), cmp1(cnt);
                // 批量展开数据/有效位索引
                std::vector<int64_t> Lval, Rval;
                if (!PRECISE) { Lval.resize(cnt); Rval.resize(cnt); }

                for (size_t off = 0; off < cnt; ++off) {
                    const size_t g = start + off;
                    const size_t i = g / rows1; // 左行
                    const size_t j = g % rows1; // 右行
                    cmp0[off] = col0[i];
                    cmp1[off] = col1[j];
                    if (!PRECISE) { Lval[off] = lvalid[i]; Rval[off] = rvalid[j]; }
                }

                const int baseTag = b * blockStride;
                // eq
                auto eqRes = BoolEqualBatchOperator(&cmp0, &cmp1, keyWidth,
                                                    /*tid*/0, /*msgTag*/baseTag,
                                                    SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

                if (PRECISE) {
                    return eqRes; // 直接返回 eq 作为 outValid
                } else {
                    // pairValid = L ∧ R
                    auto pairValid = BoolAndBatchOperator(&Lval, &Rval, 1,
                                                          /*tid*/0, /*msgTag*/baseTag + eqStride,
                                                          SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
                    // outValid = eq ∧ pairValid
                    return BoolAndBatchOperator(&eqRes, &pairValid, 1,
                                                /*tid*/0, /*msgTag*/baseTag + eqStride + andStride,
                                                SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
                }
            });
        }

        // 先把笛卡尔拼好（与原逻辑一致）
        size_t rowIndex = 0;
        for (size_t i = 0; i < rows0; ++i) {
            for (size_t j = 0; j < rows1; ++j) {
                for (size_t k = 0; k < effectiveFieldNum0; ++k)
                    joined._dataCols[k][rowIndex] = v0._dataCols[k][i];
                for (size_t k = 0; k < effectiveFieldNum1; ++k)
                    joined._dataCols[k + effectiveFieldNum0][rowIndex] = v1._dataCols[k][j];
                ++rowIndex;
            }
        }

        // 收集各批的 VALID
        rowIndex = 0;
        for (int b = 0; b < batchNum; ++b) {
            auto r = futures[b].get();
            for (size_t t = 0; t < r.size(); ++t) {
                joined._dataCols[joined.colNum() + View::VALID_COL_OFFSET][rowIndex++] = r[t];
            }
        }
    }

    // 压缩策略
    if (!BASELINE) {
        // 精确或非精确压缩：走统一清理入口（内部根据 DISABLE_PRECISE_COMPACTION 选择精确/近似）
        joined.clearInvalidEntries(0);
    }

    return joined;
}

std::vector<std::vector<std::vector<int64_t> > > Views::butterflyPermutation(
    View &view,
    int tagColIndex,
    int msgTagBase
) {
    int numBuckets = DbConf::SHUFFLE_BUCKET_NUM;
    int numLayers = static_cast<int>(std::log2(numBuckets));

    std::vector<std::vector<std::vector<int64_t> > > buckets(numBuckets);
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
                std::vector<std::vector<int64_t> > &bucket1 = buckets[idx1];
                std::vector<std::vector<int64_t> > &bucket2 = buckets[idx2];

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

                    std::vector<std::vector<int64_t> > newBucket1;
                    std::vector<std::vector<int64_t> > newBucket2;

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
            std::vector<std::vector<int64_t> > &bucket1 = buckets[indices1[i]];
            std::vector<std::vector<int64_t> > &bucket2 = buckets[indices2[i]];
            std::vector<std::vector<int64_t> > newBucket1;
            std::vector<std::vector<int64_t> > newBucket2;

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

            bucket1 = std::move(newBucket1);
            bucket2 = std::move(newBucket2);
        }
    }

    return buckets;
}

View Views::performBucketJoins(
    std::vector<std::vector<std::vector<int64_t> > > &buckets0,
    std::vector<std::vector<std::vector<int64_t> > > &buckets1,
    View &v0,
    View &v1,
    std::string &field0,
    std::string &field1
) {
    const size_t numBuckets = buckets0.size();
    bool firstResult = true;
    View result;

    for (size_t b = 0; b < numBuckets; ++b) {
        if (buckets0[b].empty() || buckets1[b].empty() ||
            buckets0[b][0].empty() || buckets1[b][0].empty()) {
            continue;
        }

        std::vector<std::string> names0 = v0._fieldNames;
        std::vector<int> widths0 = v0._fieldWidths;
        View left(v0._tableName, v0._fieldNames, v0._fieldWidths, false);
        left._dataCols = buckets0[b];
        left._dataCols.emplace_back(left.rowNum(), 0);

        std::vector<std::string> names1 = v1._fieldNames;
        std::vector<int> widths1 = v1._fieldWidths;
        View right(v1._tableName, v1._fieldNames, v1._fieldWidths, false);
        right._dataCols = buckets1[b];
        right._dataCols.emplace_back(right.rowNum(), 0);

        // if (Conf::DISABLE_MULTI_THREAD) {
        //     left.clearInvalidEntries(0);
        //     right.clearInvalidEntries(0);
        // } else {
        //     auto f = ThreadPoolSupport::submit([&left] {
        //         left.clearInvalidEntries(0);
        //     });
        //     right.clearInvalidEntries(left.clearInvalidEntriesTagStride());
        //     f.wait();
        // }

        View joined = nestedLoopJoin(left, right, field0, field1);

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

    result.clearInvalidEntries(0);

    return result;
}

int Views::butterflyPermutationTagStride(View &v) {
    size_t totalCount = v.colNum() * v.rowNum() * DbConf::SHUFFLE_BUCKET_NUM / 2;
    return static_cast<int>((totalCount + Conf::BATCH_SIZE - 1) / Conf::BATCH_SIZE) *
           BoolMutexBatchOperator::tagStride();
}

// Main shuffle bucket join function
View Views::hashJoin(View &v0, View &v1, std::string &field0, std::string &field1) {
    if (DbConf::BASELINE_MODE) {
        return nestedLoopJoin(v0, v1, field0, field1);
    }

    int numBuckets = DbConf::SHUFFLE_BUCKET_NUM;

    int tagColIndex0 = -1, tagColIndex1 = -1;

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
        return nestedLoopJoin(v0, v1, field0, field1);
    }

    std::vector<std::vector<std::vector<int64_t> > > buckets0, buckets1;
    if (Conf::DISABLE_MULTI_THREAD) {
        buckets0 = butterflyPermutation(v0, tagColIndex0, 0);
        buckets1 = butterflyPermutation(v1, tagColIndex1, 0);
    } else {
        auto f = ThreadPoolSupport::submit([&] {
            return butterflyPermutation(v0, tagColIndex0, 0);
        });
        buckets1 = butterflyPermutation(v1, tagColIndex1, butterflyPermutationTagStride(v0));
        buckets0 = f.get();
    }

    for (int i = 0; i < numBuckets; i++) {
        if (!buckets0[i].empty() && !buckets1[i].empty() &&
            !buckets0[i][0].empty() && !buckets1[i][0].empty()) {
        }
    }

    auto result = performBucketJoins(buckets0, buckets1, v0, v1, field0, field1);

    return result;
}

View Views::leftOuterJoin(View &v0, View &v1, std::string &field0, std::string &field1) {
    // 0) 内连接（保持原逻辑与 tag 分配）
    View inner = hashJoin(v0, v1, field0, field1);

    // 1) 取键列与有效列
    const int k0 = v0.colIndex(field0);
    const int k1 = v1.colIndex(field1);
    if (k0 < 0 || k1 < 0) return inner;

    const auto &left_keys  = v0._dataCols[k0];
    const auto &right_keys = v1._dataCols[k1];

    const int v0_valid_idx = v0.colNum() + View::VALID_COL_OFFSET;
    const int v1_valid_idx = v1.colNum() + View::VALID_COL_OFFSET;
    const auto &left_valid  = v0._dataCols[v0_valid_idx];
    const auto &right_valid = v1._dataCols[v1_valid_idx];

    // 2) 半连接：has_match[i] ∈ {0,1}（布尔分享）
    // 已在 in() 内根据 PRECISE/非精确路径处理了右掩蔽与左掩蔽
    std::vector<int64_t> has_match =
        in(const_cast<std::vector<int64_t>&>(left_keys),
           const_cast<std::vector<int64_t>&>(right_keys),
           const_cast<std::vector<int64_t>&>(v0._dataCols[v0_valid_idx]),
           const_cast<std::vector<int64_t>&>(v1._dataCols[v1_valid_idx]));

    // 3) 目标列结构（与 inner 一致）：v0(有效列) + v1(有效列)
    const size_t eff0 = v0.colNum() - 2;  // 去掉 VALID/PADDING
    const size_t eff1 = v1.colNum() - 2;

    std::vector<std::string> names; names.reserve(eff0 + eff1);
    std::vector<int>         widths; widths.reserve(eff0 + eff1);
    for (size_t i = 0; i < eff0; ++i) { names.push_back(v0._fieldNames[i]); widths.push_back(v0._fieldWidths[i]); }
    for (size_t i = 0; i < eff1; ++i) { names.push_back(v1._fieldNames[i]); widths.push_back(v1._fieldWidths[i]); }

    // 4) 构造 left_only：右侧全空；有效位 = left_valid ∧ ¬has_match
    View left_only(names, widths);
    const size_t n0 = v0.rowNum();

    // 4.1 左侧有效列拷贝
    for (size_t c = 0; c < eff0; ++c) {
        left_only._dataCols[c] = v0._dataCols[c];  // 与 v0 行数一致
    }
    // 4.2 右侧有效列置 0（空值）
    for (size_t c = 0; c < eff1; ++c) {
        left_only._dataCols[eff0 + c].assign(n0, 0);
    }

    // 4.3 计算有效位：not_has = left_valid ∧ ¬has_match
    std::vector<int64_t> not_has(n0);
    const int64_t mask = Comm::rank();
    for (size_t i = 0; i < n0; ++i) {
        not_has[i] = has_match[i] ^ mask;  // 先做 NOT(has_match)
    }
    // 与 left_valid 相与
    not_has = BoolAndBatchOperator(&not_has,
                                   const_cast<std::vector<int64_t>*>(&left_valid),
                                   /*bitlen=*/1, /*tid=*/0, /*msgTagBase=*/0,
                                   SecureOperator::NO_CLIENT_COMPUTE)
                    .execute()->_zis;

    // 写入 VALID / PADDING
    left_only._dataCols[left_only.colNum() + View::VALID_COL_OFFSET]   = std::move(not_has);
    left_only._dataCols[left_only.colNum() + View::PADDING_COL_OFFSET] = std::vector<int64_t>(n0, 0);

    // 5) 压缩策略：BASELINE 不压缩；精确/非精确压缩交给 clearInvalidEntries
    if (!DbConf::BASELINE_MODE) {
        left_only.clearInvalidEntries(/*msgTagBase=*/0);  // 内部按 DISABLE_PRECISE_COMPACTION 选择精确/近似
    }

    // 6) 结果 = 内连接 ∪ left_only（逐列 append；保持原有列顺序与 tag 习惯）
    View result = inner;  // 拷贝结构
    const size_t total_cols = result.colNum() + 2; // 含 VALID/PADDING
    for (size_t col = 0; col < total_cols; ++col) {
        auto &dst = result._dataCols[col];
        auto &src = left_only._dataCols[col];
        dst.insert(dst.end(), src.begin(), src.end());
    }

    // 可选：若你的外层调用希望在本函数就做物理压缩，可在非 BASELINE 下：
    // if (!DbConf::BASELINE_MODE) result.clearInvalidEntries(0);

    return result;
}

std::string Views::getAliasColName(std::string &tableName, std::string &fieldName) {
    return tableName + "." + fieldName;
}

int64_t Views::hash(int64_t keyValue) {
    return (keyValue * 31 + 17) % DbConf::SHUFFLE_BUCKET_NUM;
}

std::vector<int64_t> Views::in(std::vector<int64_t> &col1,
                               std::vector<int64_t> &col2,
                               std::vector<int64_t> &left_valid,
                               std::vector<int64_t> &right_valid) {
    if (Conf::BATCH_SIZE <= 0 || Conf::DISABLE_MULTI_THREAD) {
        return inSingleBatch(col1, col2, left_valid, right_valid);
    } else {
        return inMultiBatches(col1, col2, left_valid, right_valid);
    }
}

void Views::revealAndPrint(View &v) {
    for (int i = 0; i < v.colNum(); i++) {
        if (Comm::rank() == 1) {
            Comm::serverSend(v._dataCols[i], 64, 0);
        } else {
            std::vector<int64_t> temp;
            Comm::serverReceive(temp, 64, 0);
            for (int j = 0; j < temp.size(); j++) {
                temp[j] ^= v._dataCols[i][j];
            }
            Log::i("{}: {}", v._fieldNames[i], StringUtils::vecToString(temp));
        }
    }
}

// 三种模式的统一版：
// - 精确压缩：  left_valid == nullptr 且 right_valid == nullptr
// - BASELINE：  传入左右 valid（避免无效行误命中）
// - 非精确压缩：同 BASELINE，传入左右 valid
// 统一版 in()，支持 BASELINE / 精确压缩 / 非精确压缩
// - 精确压缩：跳过 valid 掩蔽（视为全 1），减少不必要计算
// - BASELINE/非精确压缩：右掩蔽参与匹配的等式，左掩蔽输出
std::vector<int64_t> Views::inSingleBatch(std::vector<int64_t> &col1,
                                          std::vector<int64_t> &col2,
                                          std::vector<int64_t> &left_valid,
                                          std::vector<int64_t> &right_valid) {
    const size_t n = col1.size();
    const size_t m = col2.size();
    std::vector<int64_t> result(n, 0);
    if (n == 0 || m == 0) return result;

    const int64_t rankShare = Comm::rank();

    // 是否走“精确压缩”（视为无无效行，跳过 valid 掩蔽）
    const bool PRECISE = (!DbConf::BASELINE_MODE) && (!DbConf::DISABLE_PRECISE_COMPACTION);

    // 计算归约轮数 L（每轮把每行列数减半，用于分配与规避 tag 段）
    size_t cols = m;
    int L = 0;
    while (cols > 1) {
        cols = cols / 2 + (cols & 1);
        ++L;
    }

    const int eqStride = BoolEqualBatchOperator::tagStride();
    const int andStride = BoolAndBatchOperator::tagStride();
    const int tagStride = std::max(eqStride, andStride);

    // ---------- 1) 一次性等值比较：得到 n*m 的 equal_all ----------
    std::vector<int64_t> bigLeft(n * m), bigRight(n * m);
    for (size_t i = 0; i < n; ++i) {
        std::fill_n(bigLeft.begin() + i * m, m, col1[i]);
        std::copy(col2.begin(), col2.end(), bigRight.begin() + i * m);
    }
    auto equal_all = BoolEqualBatchOperator(&bigLeft, &bigRight,
                                            /*width=*/64, /*tid=*/0,
                                            /*msgTagOffset=*/0,
                                            SecureOperator::NO_CLIENT_COMPUTE)
            .execute()->_zis; // size = n*m

    // ---------- 2) BASELINE/非精确压缩：右掩蔽（无效右行不参与匹配） ----------
    if (!PRECISE) {
        if (right_valid.size() != m) {
            Log::e("inSingleBatch: right_valid size mismatch: got {}, expect {}", right_valid.size(), m);
        } else {
            std::vector<int64_t> bigRightValid(n * m);
            for (size_t i = 0; i < n; ++i) {
                std::copy(right_valid.begin(), right_valid.end(),
                          bigRightValid.begin() + i * m);
            }
            equal_all = BoolAndBatchOperator(&equal_all, &bigRightValid,
                                             /*width=*/1, /*tid=*/0,
                                             /*msgTagOffset=*/(L + 1) * tagStride,
                                             SecureOperator::NO_CLIENT_COMPUTE)
                    .execute()->_zis;
        }
    }

    // ---------- 3) NOT(equal) 并做行内树形 AND 归约 ----------
    for (auto &b: equal_all) b ^= rankShare; // NOT(equal)
    std::vector<int64_t> cur = std::move(equal_all);

    cols = m;
    int round = 0;
    std::vector<int64_t> andLeft, andRight, next;

    while (cols > 1) {
        const size_t pairsPerRow = cols / 2;
        const bool hasOdd = (cols & 1);
        const size_t totalPairs = n * pairsPerRow;

        andLeft.resize(totalPairs);
        andRight.resize(totalPairs);

        size_t w = 0;
        for (size_t i = 0; i < n; ++i) {
            const size_t base = i * cols;
            for (size_t p = 0; p < pairsPerRow; ++p) {
                andLeft[w] = cur[base + 2 * p];
                andRight[w] = cur[base + 2 * p + 1];
                ++w;
            }
        }

        auto andOut = BoolAndBatchOperator(&andLeft, &andRight,
                                           /*width=*/1, /*tid=*/0,
                                           /*msgTagOffset=*/(round + 1) * tagStride,
                                           SecureOperator::NO_CLIENT_COMPUTE)
                .execute()->_zis;

        next.resize(n * (pairsPerRow + (hasOdd ? 1 : 0)));
        w = 0;
        for (size_t i = 0; i < n; ++i) {
            const size_t nextBase = i * (pairsPerRow + (hasOdd ? 1 : 0));
            for (size_t p = 0; p < pairsPerRow; ++p) {
                next[nextBase + p] = andOut[w++];
            }
            if (hasOdd) next[nextBase + pairsPerRow] = cur[i * cols + (cols - 1)];
        }

        cur.swap(next);
        cols = pairsPerRow + (hasOdd ? 1 : 0);
        ++round;
    }

    // cur[i] = AND_j NOT(equal(i,j))
    // result[i] = NOT(cur[i]) = 存在至少一个右行与 col1[i] 相等（且在 BASELINE/非精确下右行有效）
    for (size_t i = 0; i < n; ++i) result[i] = cur[i] ^ rankShare;

    // ---------- 4) BASELINE/非精确压缩：左掩蔽输出（无效左行 -> 0） ----------
    if (!PRECISE) {
        if (left_valid.size() != n) {
            Log::e("inSingleBatch: left_valid size mismatch: got {}, expect {}", left_valid.size(), n);
        } else {
            result = BoolAndBatchOperator(&result, &left_valid,
                                          /*width=*/1, /*tid=*/0,
                                          /*msgTagOffset=*/(L + 2) * tagStride,
                                          SecureOperator::NO_CLIENT_COMPUTE)
                    .execute()->_zis;
        }
    }

    return result;
}

std::vector<int64_t> Views::inMultiBatches(std::vector<int64_t> &col1,
                                           std::vector<int64_t> &col2,
                                           std::vector<int64_t> &left_valid,
                                           std::vector<int64_t> &right_valid) {
    std::vector<int64_t> result(col1.size(), 0);
    const size_t n = col1.size();
    const size_t m = col2.size();
    if (n == 0 || m == 0) return result;

    const int64_t rankShare = Comm::rank();
    const size_t totalSize = n * m;
    const int batchSize = Conf::BATCH_SIZE;
    const int numBatches = (totalSize + batchSize - 1) / batchSize;

    // 是否“精确压缩”：视为无无效行，跳过 valid 掩蔽
    const bool PRECISE = (!DbConf::BASELINE_MODE) && (!DbConf::DISABLE_PRECISE_COMPACTION);

    // 计算等值轮数（树形 AND 归约的层数），用于给 AND 分配独立 tag 段
    size_t cols = m;
    int reduceRounds = 0;
    while (cols > 1) {
        cols = cols / 2 + (cols & 1);
        ++reduceRounds;
    }

    const int equalTagStride = BoolEqualBatchOperator::tagStride();
    const int andTagStride   = BoolAndBatchOperator::tagStride();

    // ---------- 第一步：分批做等值比较（必要时顺带右掩蔽） ----------
    std::vector<std::future<std::vector<int64_t>>> equalFutures(numBatches);

    for (int b = 0; b < numBatches; ++b) {
        equalFutures[b] = ThreadPoolSupport::submit([&, b]() {
            const size_t start = static_cast<size_t>(b) * batchSize;
            const size_t end   = std::min(totalSize, start + batchSize);
            const size_t cnt   = end - start;

            std::vector<int64_t> batchLeft(cnt), batchRight(cnt);

            // 填充当前批次的 (i,j) 映射
            for (size_t idx = start; idx < end; ++idx) {
                const size_t i = idx / m; // 行（来自 col1）
                const size_t j = idx % m; // 列（来自 col2）
                batchLeft[idx - start]  = col1[i];
                batchRight[idx - start] = col2[j];
            }

            // 等值比较：独立 tag 段，避免与其它批次/算子重叠
            const int eqTag = b * (equalTagStride + (PRECISE ? 0 : andTagStride));
            auto eqRes = BoolEqualBatchOperator(&batchLeft, &batchRight,
                                                /*width=*/64, /*tid=*/0,
                                                /*msgTagOffset=*/eqTag,
                                                SecureOperator::NO_CLIENT_COMPUTE)
                         .execute()->_zis;

            if (!PRECISE) {
                // 右掩蔽：把无效右行的匹配置 0
                if (right_valid.size() == m) {
                    std::vector<int64_t> rv(cnt);
                    for (size_t idx = start; idx < end; ++idx) {
                        const size_t j = idx % m;
                        rv[idx - start] = right_valid[j];
                    }
                    const int andTag = eqTag + equalTagStride;
                    eqRes = BoolAndBatchOperator(&eqRes, &rv,
                                                 /*width=*/1, /*tid=*/0,
                                                 /*msgTagOffset=*/andTag,
                                                 SecureOperator::NO_CLIENT_COMPUTE)
                            .execute()->_zis;
                } else {
                    Log::e("inMultiBatches: right_valid size mismatch: got {}, expect {}", right_valid.size(), m);
                }
            }

            // 取反：NOT(equal) 用本方异或掩码
            for (auto &bit : eqRes) bit ^= rankShare;
            return eqRes;
        });
    }

    // 收集所有批次的 NOT(equal) 结果，按 n 行 × m 列扁平化存入 cur
    std::vector<int64_t> cur;
    cur.reserve(totalSize);
    for (auto &f : equalFutures) {
        auto part = f.get();
        cur.insert(cur.end(), part.begin(), part.end());
    }

    // ---------- 第二步：行内树形 AND 归约（把每行 m 个 NOT(equal) 与成 1 个） ----------
    size_t curCols = m;
    int round = 0;

    while (curCols > 1) {
        const size_t pairsPerRow = curCols / 2;
        const bool   hasOdd      = (curCols & 1);
        const size_t totalPairs  = n * pairsPerRow;

        if (totalPairs == 0) break;

        // 当本轮 pair 数量较小，可单批做 AND；否则拆分多批并行
        if (totalPairs <= static_cast<size_t>(batchSize)) {
            std::vector<int64_t> andLeft(totalPairs), andRight(totalPairs);

            size_t w = 0;
            for (size_t i = 0; i < n; ++i) {
                const size_t base = i * curCols;
                for (size_t p = 0; p < pairsPerRow; ++p) {
                    andLeft[w]  = cur[base + 2 * p];
                    andRight[w] = cur[base + 2 * p + 1];
                    ++w;
                }
            }

            // 归约 AND：等值批次用掉了 numBatches * (equalTagStride + (PRECISE?0:andTagStride)) 的空间
            const int andBaseTag = numBatches * (equalTagStride + (PRECISE ? 0 : andTagStride))
                                   + round * andTagStride;
            auto andOut = BoolAndBatchOperator(&andLeft, &andRight,
                                               /*width=*/1, /*tid=*/0,
                                               /*msgTagOffset=*/andBaseTag,
                                               SecureOperator::NO_CLIENT_COMPUTE)
                          .execute()->_zis;

            // 组装下一轮（奇数列末尾直接带过去）
            std::vector<int64_t> next(n * (pairsPerRow + (hasOdd ? 1 : 0)));
            w = 0;
            for (size_t i = 0; i < n; ++i) {
                const size_t nextBase = i * (pairsPerRow + (hasOdd ? 1 : 0));
                for (size_t p = 0; p < pairsPerRow; ++p) {
                    next[nextBase + p] = andOut[w++];
                }
                if (hasOdd) {
                    next[nextBase + pairsPerRow] = cur[i * curCols + (curCols - 1)];
                }
            }
            cur.swap(next);
        } else {
            const int andBatches = (totalPairs + batchSize - 1) / batchSize;
            std::vector<std::future<std::vector<int64_t>>> andFuts(andBatches);

            for (int b = 0; b < andBatches; ++b) {
                andFuts[b] = ThreadPoolSupport::submit([&, b, round]() {
                    const size_t start = static_cast<size_t>(b) * batchSize;
                    const size_t end   = std::min(totalPairs, start + batchSize);
                    const size_t cnt   = end - start;

                    std::vector<int64_t> andLeft(cnt), andRight(cnt);

                    // 将“全局 pair 索引 -> (row, pairInRow)” 映射到 cur 的两个元素
                    size_t globalPairIdx = start;
                    for (size_t t = 0; t < cnt; ++t, ++globalPairIdx) {
                        size_t row = globalPairIdx / pairsPerRow;
                        size_t p   = globalPairIdx % pairsPerRow;
                        const size_t base = row * curCols;
                        andLeft[t]  = cur[base + 2 * p];
                        andRight[t] = cur[base + 2 * p + 1];
                    }

                    const int andTag = numBatches * (equalTagStride + (PRECISE ? 0 : andTagStride))
                                       + round * andTagStride
                                       + b * andTagStride;
                    return BoolAndBatchOperator(&andLeft, &andRight,
                                                /*width=*/1, /*tid=*/0,
                                                /*msgTagOffset=*/andTag,
                                                SecureOperator::NO_CLIENT_COMPUTE)
                           .execute()->_zis;
                });
            }

            // 收集 AND 结果并组装下一轮
            std::vector<int64_t> andOut;
            andOut.reserve(totalPairs);
            for (auto &f : andFuts) {
                auto part = f.get();
                andOut.insert(andOut.end(), part.begin(), part.end());
            }

            std::vector<int64_t> next(n * (pairsPerRow + (hasOdd ? 1 : 0)));
            size_t w = 0;
            for (size_t i = 0; i < n; ++i) {
                const size_t nextBase = i * (pairsPerRow + (hasOdd ? 1 : 0));
                for (size_t p = 0; p < pairsPerRow; ++p) {
                    next[nextBase + p] = andOut[w++];
                }
                if (hasOdd) {
                    next[nextBase + pairsPerRow] = cur[i * curCols + (curCols - 1)];
                }
            }
            cur.swap(next);
        }

        curCols = pairsPerRow + (hasOdd ? 1 : 0);
        ++round;
    }

    // 此时 cur[i] = AND_j NOT(equal(i,j))
    // IN = NOT(cur[i])
    result.resize(n);
    for (size_t i = 0; i < n; ++i) result[i] = cur[i] ^ rankShare;

    // ---------- 第三步：非精确路径做左掩蔽 ----------
    if (!PRECISE) {
        if (left_valid.size() == n) {
            const int finalMaskTag =
                numBatches * (equalTagStride + andTagStride)  // 等值 + 右掩蔽
                + reduceRounds * andTagStride;               // 所有归约轮
            result = BoolAndBatchOperator(&result, &left_valid,
                                          /*width=*/1, /*tid=*/0,
                                          /*msgTagOffset=*/finalMaskTag,
                                          SecureOperator::NO_CLIENT_COMPUTE)
                     .execute()->_zis;
        } else {
            Log::e("inMultiBatches: left_valid size mismatch: got {}, expect {}", left_valid.size(), n);
        }
    }

    return result;
}

void Views::addRedundantCols(View &v) {
    v._fieldNames.emplace_back(View::VALID_COL_NAME);
    v._fieldWidths.emplace_back(1);
    v._dataCols.emplace_back(v._dataCols[0].size());

    v._fieldNames.emplace_back(View::PADDING_COL_NAME);
    v._fieldWidths.emplace_back(1);
    v._dataCols.emplace_back(v._dataCols[0].size());
}
