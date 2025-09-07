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
    size_t effectiveFieldNum0 = v0.colNum() - 2;
    size_t effectiveFieldNum1 = v1.colNum() - 2;

    std::vector<std::string> fieldNames(effectiveFieldNum0 + effectiveFieldNum1);
    std::string tableName0 = v0._tableName.empty() ? "$t0" : v0._tableName;
    std::string tableName1 = v1._tableName.empty() ? "$t1" : v1._tableName;

    const std::string prefix = Table::BUCKET_TAG_PREFIX;

    auto decorate = [&](const std::string& tableName, const std::string& raw) -> std::string {
        if (raw.compare(0, prefix.size(), prefix) == 0) { // 以 BUCKET_TAG_PREFIX 开头
            return prefix + tableName + "." + raw.substr(prefix.size());
        } else {
            return tableName + "." + raw;
        }
    };

    for (int i = 0; i < effectiveFieldNum0; ++i) {
        fieldNames[i] = decorate(tableName0, v0._fieldNames[i]);
    }
    for (int i = 0; i < effectiveFieldNum1; ++i) {
        fieldNames[i + effectiveFieldNum0] = decorate(tableName1, v1._fieldNames[i]);
    }

    std::vector<int> fieldWidths(effectiveFieldNum0 + effectiveFieldNum1);
    for (int i = 0; i < effectiveFieldNum0; ++i) {
        fieldWidths[i] = v0._fieldWidths[i];
    }
    for (int i = 0; i < effectiveFieldNum1; ++i) {
        fieldWidths[i + effectiveFieldNum0] = v1._fieldWidths[i];
    }

    View joined(fieldNames, fieldWidths);
    if (v0._dataCols.empty() || v1._dataCols.empty()) {
        return joined;
    }

    size_t rows0 = v0.rowNum();
    size_t rows1 = v1.rowNum();

    if (rows0 == 0 || rows1 == 0) {
        return joined;
    }

    size_t n = rows0 * rows1;
    for (int i = 0; i < joined.colNum(); ++i) {
        joined._dataCols[i].resize(n);
    }

    int colIndex0 = v0.colIndex(field0);
    int colIndex1 = v1.colIndex(field1);

    if (colIndex0 == -1 || colIndex1 == -1) {
        // Field not found, return empty result
        return joined;
    }

    auto &col0 = v0._dataCols[colIndex0];
    auto &col1 = v1._dataCols[colIndex1];

    int batchSize = Conf::BATCH_SIZE;
    if (batchSize <= 0 || Conf::DISABLE_MULTI_THREAD) {
        size_t rowIndex = 0;
        std::vector<int64_t> cmp0, cmp1;
        cmp0.reserve(n);
        cmp1.reserve(n);

        for (size_t i = 0; i < rows0; ++i) {
            for (size_t j = 0; j < rows1; ++j) {
                // For each column
                for (size_t k = 0; k < effectiveFieldNum0; ++k) {
                    joined._dataCols[k][rowIndex] = v0._dataCols[k][i];
                }
                for (size_t k = 0; k < effectiveFieldNum1; ++k) {
                    joined._dataCols[k + effectiveFieldNum0][rowIndex] = v1._dataCols[k][j];
                }
                cmp0.push_back(col0[i]);
                cmp1.push_back(col1[j]);
                rowIndex++;
            }
        }

        joined._dataCols[joined.colNum() + View::VALID_COL_OFFSET] = BoolEqualBatchOperator(
            &cmp0, &cmp1, v0._fieldWidths[colIndex0], 0, 0, SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    } else {
        size_t batchNum = (n + batchSize - 1) / batchSize;
        std::vector<std::future<std::vector<int64_t> > > futures(batchNum);

        int width = v0._fieldWidths[colIndex0];
        for (int b = 0; b < batchNum; ++b) {
            futures[b] = ThreadPoolSupport::submit([b, batchSize, rows0, rows1, &col0, &col1, width] {
                std::vector<int64_t> cmp0, cmp1;
                cmp0.reserve(batchSize);
                cmp1.reserve(batchSize);

                for (int i = 0; i < batchSize; ++i) {
                    int temp = b * batchSize + i;
                    if (temp / rows1 == rows0) {
                        break;
                    }
                    cmp0.push_back(col0[temp / rows1]);
                    cmp1.push_back(col1[temp % rows1]);
                }

                return BoolEqualBatchOperator(
                    &cmp0, &cmp1, width, 0, b * BoolEqualBatchOperator::tagStride(),
                    SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
            });
        }

        size_t rowIndex = 0;
        for (size_t i = 0; i < rows0; ++i) {
            for (size_t j = 0; j < rows1; ++j) {
                // For each column
                for (size_t k = 0; k < effectiveFieldNum0; ++k) {
                    joined._dataCols[k][rowIndex] = v0._dataCols[k][i];
                }
                for (size_t k = 0; k < effectiveFieldNum1; ++k) {
                    joined._dataCols[k + effectiveFieldNum0][rowIndex] = v1._dataCols[k][j];
                }
                rowIndex++;
            }
        }

        rowIndex = 0;
        for (int b = 0; b < batchNum; ++b) {
            auto r = futures[b].get();
            for (int j = 0; j < r.size(); j++) {
                joined._dataCols[joined.colNum() + View::VALID_COL_OFFSET][rowIndex++] = r[j];
            }
        }
    }

    if (!DbConf::BASELINE_MODE) {
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

        left.clearInvalidEntries(0);
        right.clearInvalidEntries(0);

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
        if (v0._fieldNames[i] == View::BUCKET_TAG_PREFIX + field0) {
            tagColIndex0 = i;
            break;
        }
    }

    for (int i = 0; i < v1._fieldNames.size(); i++) {
        if (v0._fieldNames[i] == View::BUCKET_TAG_PREFIX + field1) {
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
    // 0) 内连接
    View inner = hashJoin(v0, v1, field0, field1);

    // 1) 半连接标记：左行是否在右表有匹配
    const int k0 = v0.colIndex(field0);
    const int k1 = v1.colIndex(field1);
    if (k0 < 0 || k1 < 0) return inner;

    const auto &left_keys  = v0._dataCols[k0];
    const auto &right_keys = v1._dataCols[k1];
    // has_match[i] ∈ {0,1} (布尔份额)
    std::vector<int64_t> has_match = in(const_cast<std::vector<int64_t>&>(left_keys),
                                        const_cast<std::vector<int64_t>&>(right_keys));

    // 2) 目标列结构（与 inner 一致）：v0(有效列) + v1(有效列)
    const size_t eff0 = v0.colNum() - 2;  // 去掉 VALID/PADDING
    const size_t eff1 = v1.colNum() - 2;

    std::vector<std::string> names; names.reserve(eff0 + eff1);
    std::vector<int>         widths; widths.reserve(eff0 + eff1);
    for (size_t i = 0; i < eff0; ++i) { names.push_back(v0._fieldNames[i]); widths.push_back(v0._fieldWidths[i]); }
    for (size_t i = 0; i < eff1; ++i) { names.push_back(v1._fieldNames[i]); widths.push_back(v1._fieldWidths[i]); }

    // 3) 构造 left_only（右侧全空）：行有效位 = NOT(has_match)
    View left_only(names, widths);
    const size_t n0 = v0.rowNum();

    // 3.1 左侧有效列逐列拷贝
    for (size_t c = 0; c < eff0; ++c) {
        left_only._dataCols[c] = v0._dataCols[c];               // 与 v0 行数一致
    }
    // 3.2 右侧有效列置 0（空值）；如果你支持 null-bit，可另加一列 right_is_null = NOT(has_match)
    for (size_t c = 0; c < eff1; ++c) {
        left_only._dataCols[eff0 + c].assign(n0, 0);            // 布尔/算术 0 的秘密份额皆可
    }

    // 3.3 设置有效位：NOT(has_match)
    // 你的布尔“取反”写法之前用的是 bs ^ Comm::rank()
    std::vector<int64_t> not_has(n0);
    for (size_t i = 0; i < n0; ++i) not_has[i] = has_match[i] ^ Comm::rank();

    // 写入 VALID / PADDING
    left_only._dataCols[left_only.colNum() + View::VALID_COL_OFFSET]   = not_has;
    left_only._dataCols[left_only.colNum() + View::PADDING_COL_OFFSET] = std::vector<int64_t>(n0, 0);

    if (!DbConf::BASELINE_MODE) {
        // 3.4 仅保留无匹配行
        left_only.clearInvalidEntries(0);
    }

    // 4) 结果 = 内连接 ∪ left_only
    // 若你有封装好的拼接函数，用它；否则逐列 append
    View result = inner;  // 复制列结构
    const size_t total_cols = result.colNum() + 2; // 含 VALID/PADDING
    for (size_t col = 0; col < total_cols; ++col) {
        auto &dst = result._dataCols[col];
        auto &src = left_only._dataCols[col];
        dst.insert(dst.end(), src.begin(), src.end());
    }
    return result;
}

std::string Views::getAliasColName(std::string &tableName, std::string &fieldName) {
    return tableName + "." + fieldName;
}

int64_t Views::hash(int64_t keyValue) {
    return (keyValue * 31 + 17) % DbConf::SHUFFLE_BUCKET_NUM;
}

std::vector<int64_t> Views::in(std::vector<int64_t> &col1, std::vector<int64_t> &col2) {
    if (Conf::BATCH_SIZE <= 0 || Conf::DISABLE_MULTI_THREAD) {
        return inSingleBatch(col1, col2);
    } else {
        return inMultiBatches(col1, col2);
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

std::vector<int64_t> Views::inSingleBatch(std::vector<int64_t> &col1, std::vector<int64_t> &col2) {
    std::vector<int64_t> result(col1.size(), 0);
    if (col1.empty() || col2.empty()) return result;

    const size_t n = col1.size();
    const size_t m = col2.size();
    const int64_t rankShare = Comm::rank();
    const int tagStride = std::max(BoolEqualBatchOperator::tagStride(),
                                   BoolAndBatchOperator::tagStride());

    // ---------- 第一步：一次性做等值比较 ----------
    // bigLeft:  [ col1[0]重复m次 | col1[1]重复m次 | ... | col1[n-1]重复m次 ]  (共 n*m)
    // bigRight: [ col2[0..m-1] | col2[0..m-1] | ... | col2[0..m-1] ]       (共 n*m)
    std::vector<int64_t> bigLeft(n * m), bigRight(n * m);
    for (size_t i = 0; i < n; ++i) {
        std::fill_n(bigLeft.begin() + i * m, m, col1[i]);
        std::copy(col2.begin(), col2.end(), bigRight.begin() + i * m);
    }

    // 一次调用：得到所有 equal 结果（布尔XOR共享，0/1）
    auto equal_all = BoolEqualBatchOperator(&bigLeft, &bigRight,
                                            /*width=*/64, /*taskTag=*/0, /*msgTagOffset=*/0,
                                            SecureOperator::NO_CLIENT_COMPUTE)
            .execute()->_zis; // size == n*m

    // ---------- 第二步：NOT(equal)（共享层面取反：b ^= 1 由单方翻转实现） ----------
    for (auto &b: equal_all) b ^= rankShare; // now is "not_equal_all"

    // ---------- 第三步：对每个 i 的整行做树形 AND（每轮一次批量 AND） ----------
    // cur 视为 n 行、每行 m 列的扁平矩阵，按行连排
    std::vector<int64_t> cur = std::move(equal_all);
    size_t cols = m;
    int round = 0;

    std::vector<int64_t> andLeft; // 本轮所有 pair 的左输入（拼成一个大向量）
    std::vector<int64_t> andRight; // 本轮所有 pair 的右输入
    std::vector<int64_t> next; // 本轮归约后的结果（每行列数减半，奇数多带1）

    while (cols > 1) {
        const size_t pairsPerRow = cols / 2;
        const bool hasOdd = (cols & 1);
        const size_t totalPairs = n * pairsPerRow;

        andLeft.resize(totalPairs);
        andRight.resize(totalPairs);

        // 打包所有行的 (2p, 2p+1) 为本轮的 AND 输入
        size_t w = 0;
        for (size_t i = 0; i < n; ++i) {
            const size_t base = i * cols;
            for (size_t p = 0; p < pairsPerRow; ++p) {
                andLeft[w] = cur[base + (2 * p)];
                andRight[w] = cur[base + (2 * p + 1)];
                ++w;
            }
        }

        // 一次调用：本轮所有配对的 AND
        auto andOut = BoolAndBatchOperator(&andLeft, &andRight,
                                           /*widthPerElem=*/1, /*taskTag=*/0,
                                           /*msgTagOffset=*/(round + 1) * tagStride,
                                           SecureOperator::NO_CLIENT_COMPUTE)
                .execute()->_zis; // size == totalPairs

        // 组装下一轮（保留每行最后一个"奇数尾"）
        next.resize(n * (pairsPerRow + (hasOdd ? 1 : 0)));
        w = 0;
        for (size_t i = 0; i < n; ++i) {
            const size_t nextBase = i * (pairsPerRow + (hasOdd ? 1 : 0));
            for (size_t p = 0; p < pairsPerRow; ++p) {
                next[nextBase + p] = andOut[w++];
            }
            if (hasOdd) {
                next[nextBase + pairsPerRow] = cur[i * cols + (cols - 1)];
            }
        }

        cur.swap(next);
        cols = pairsPerRow + (hasOdd ? 1 : 0);
        ++round;
    }

    // ---------- 第四步：IN = NOT( AND_j NOT(equal_ij) ) ----------
    result.resize(n);
    for (size_t i = 0; i < n; ++i) {
        result[i] = cur[i] ^ rankShare; // 共享层面取反得到 IN
    }
    return result;
}

std::vector<int64_t> Views::inMultiBatches(std::vector<int64_t> &col1, std::vector<int64_t> &col2) {
    std::vector<int64_t> result(col1.size(), 0);
    if (col1.empty() || col2.empty()) return result;

    const size_t n = col1.size();
    const size_t m = col2.size();
    const int64_t rankShare = Comm::rank();
    const size_t totalSize = n * m;
    const int batchSize = Conf::BATCH_SIZE;
    const int numBatches = (totalSize + batchSize - 1) / batchSize;

    // Calculate tag stride to avoid overlap
    const int equalTagStride = BoolEqualBatchOperator::tagStride();
    const int andTagStride = BoolAndBatchOperator::tagStride();
    const int maxTagStride = std::max(equalTagStride, andTagStride);

    // ---------- 第一步：分批次做等值比较 ----------
    std::vector<std::future<std::vector<int64_t> > > equalFutures(numBatches);

    for (int b = 0; b < numBatches; ++b) {
        equalFutures[b] = ThreadPoolSupport::submit([&, b] {
            const size_t start = static_cast<size_t>(b) * batchSize;
            const size_t end = std::min(totalSize, start + batchSize);
            const size_t cnt = end - start;

            std::vector<int64_t> batchLeft(cnt), batchRight(cnt);

            // 填充当前批次的数据
            for (size_t idx = start; idx < end; ++idx) {
                const size_t i = idx / m; // col1 的索引
                const size_t j = idx % m; // col2 的索引
                batchLeft[idx - start] = col1[i];
                batchRight[idx - start] = col2[j];
            }

            // 执行等值比较，使用不同的 msgTagOffset 避免重叠
            auto equalResults = BoolEqualBatchOperator(&batchLeft, &batchRight,
                                                       /*width=*/64, /*taskTag=*/0,
                                                       /*msgTagOffset=*/b * equalTagStride,
                                                       SecureOperator::NO_CLIENT_COMPUTE)
                    .execute()->_zis;

            // NOT(equal) - 共享层面取反
            for (auto &b: equalResults) {
                b ^= rankShare;
            }

            return equalResults;
        });
    }

    // 收集所有批次的结果
    std::vector<int64_t> equal_all;
    equal_all.reserve(totalSize);
    for (auto &future: equalFutures) {
        auto batchResult = future.get();
        equal_all.insert(equal_all.end(), batchResult.begin(), batchResult.end());
    }

    // ---------- 第二步：对每个 i 的整行做树形 AND（分批并行处理） ----------
    std::vector<int64_t> cur = std::move(equal_all);
    size_t cols = m;
    int round = 0;

    while (cols > 1) {
        const size_t pairsPerRow = cols / 2;
        const bool hasOdd = (cols & 1);
        const size_t totalPairs = n * pairsPerRow;

        // 如果总对数较小，直接单批处理
        if (totalPairs <= static_cast<size_t>(batchSize)) {
            std::vector<int64_t> andLeft(totalPairs), andRight(totalPairs);

            size_t w = 0;
            for (size_t i = 0; i < n; ++i) {
                const size_t base = i * cols;
                for (size_t p = 0; p < pairsPerRow; ++p) {
                    andLeft[w] = cur[base + (2 * p)];
                    andRight[w] = cur[base + (2 * p + 1)];
                    ++w;
                }
            }

            auto andOut = BoolAndBatchOperator(&andLeft, &andRight,
                                               /*widthPerElem=*/1, /*taskTag=*/0,
                                               /*msgTagOffset=*/numBatches * equalTagStride + round * andTagStride,
                                               SecureOperator::NO_CLIENT_COMPUTE)
                    .execute()->_zis;

            // 组装下一轮
            std::vector<int64_t> next(n * (pairsPerRow + (hasOdd ? 1 : 0)));
            w = 0;
            for (size_t i = 0; i < n; ++i) {
                const size_t nextBase = i * (pairsPerRow + (hasOdd ? 1 : 0));
                for (size_t p = 0; p < pairsPerRow; ++p) {
                    next[nextBase + p] = andOut[w++];
                }
                if (hasOdd) {
                    next[nextBase + pairsPerRow] = cur[i * cols + (cols - 1)];
                }
            }

            cur.swap(next);
        } else {
            // 分批处理 AND 操作
            const int andBatches = (totalPairs + batchSize - 1) / batchSize;
            std::vector<std::future<std::vector<int64_t> > > andFutures(andBatches);

            for (int b = 0; b < andBatches; ++b) {
                andFutures[b] = ThreadPoolSupport::submit([&, b, round] {
                    const size_t start = static_cast<size_t>(b) * batchSize;
                    const size_t end = std::min(totalPairs, start + batchSize);
                    const size_t cnt = end - start;

                    std::vector<int64_t> batchLeft(cnt), batchRight(cnt);

                    // 计算当前批次对应的行和列位置
                    size_t w = 0;
                    size_t globalPairIdx = start;
                    for (size_t localIdx = 0; localIdx < cnt; ++localIdx, ++globalPairIdx) {
                        // 找到这个全局pair索引对应的行和列
                        size_t rowIdx = 0;
                        size_t remainingPairs = globalPairIdx;

                        // 找到对应的行
                        while (remainingPairs >= pairsPerRow) {
                            remainingPairs -= pairsPerRow;
                            rowIdx++;
                        }

                        const size_t pairInRow = remainingPairs;
                        const size_t base = rowIdx * cols;

                        batchLeft[localIdx] = cur[base + (2 * pairInRow)];
                        batchRight[localIdx] = cur[base + (2 * pairInRow + 1)];
                    }

                    return BoolAndBatchOperator(&batchLeft, &batchRight,
                                                /*widthPerElem=*/1, /*taskTag=*/0,
                                                /*msgTagOffset=*/
                                                numBatches * equalTagStride + round * andTagStride + b * andTagStride,
                                                SecureOperator::NO_CLIENT_COMPUTE)
                            .execute()->_zis;
                });
            }

            // 收集 AND 结果
            std::vector<int64_t> andOut;
            andOut.reserve(totalPairs);
            for (auto &future: andFutures) {
                auto batchResult = future.get();
                andOut.insert(andOut.end(), batchResult.begin(), batchResult.end());
            }

            // 组装下一轮
            std::vector<int64_t> next(n * (pairsPerRow + (hasOdd ? 1 : 0)));
            size_t w = 0;
            for (size_t i = 0; i < n; ++i) {
                const size_t nextBase = i * (pairsPerRow + (hasOdd ? 1 : 0));
                for (size_t p = 0; p < pairsPerRow; ++p) {
                    next[nextBase + p] = andOut[w++];
                }
                if (hasOdd) {
                    next[nextBase + pairsPerRow] = cur[i * cols + (cols - 1)];
                }
            }

            cur.swap(next);
        }

        cols = pairsPerRow + (hasOdd ? 1 : 0);
        ++round;
    }

    // ---------- 第三步：IN = NOT( AND_j NOT(equal_ij) ) ----------
    result.resize(n);
    for (size_t i = 0; i < n; ++i) {
        result[i] = cur[i] ^ rankShare; // 共享层面取反得到 IN
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
