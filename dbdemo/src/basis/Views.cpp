//
// Created by 杜建璋 on 25-6-2.
//

#include "../../include/basis/Views.h"

#include "compute/batch/bool/BoolEqualBatchOperator.h"
#include "compute/batch/bool/BoolXorBatchOperator.h"
#include "compute/batch/bool/BoolMutexBatchOperator.h"
#include "conf/DbConf.h"
#include "parallel/ThreadPoolSupport.h"
#include "utils/Math.h"
#include "comm/Comm.h"
#include "secret/Secrets.h"
#include "utils/StringUtils.h"
#include "utils/Log.h"
#include <cmath>

View Views::selectAll(Table &t) {
    View v(t._tableName, t._fieldNames, t._fieldWidths);
    v._dataCols = t._dataCols;
    v._dataCols.emplace_back(t._dataCols[0].size(), Comm::rank());
    v._dataCols.emplace_back(t._dataCols[0].size());
    return v;
}

View Views::selectAllWithFieldPrefix(Table &t) {
    std::vector<std::string> fieldNames(t._fieldNames.size());
    for (int i = 0; i < t._fieldNames.size() - 2; i++) {
        if (fieldNames[i] != View::VALID_COL_NAME && fieldNames[i] != View::PADDING_COL_NAME) {
            fieldNames[i] = getAliasColName(t._tableName, t._fieldNames[i]);
        } else {
            fieldNames[i] = t._fieldNames[i];
        }
    }

    View v(t._tableName, fieldNames, t._fieldWidths);
    v._dataCols = t._dataCols;
    v._dataCols.emplace_back(t._dataCols[0].size(), Comm::rank());
    v._dataCols.emplace_back(t._dataCols[0].size());
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

    v._dataCols.reserve(indices.size() + 2);
    for (auto idx: indices) {
        v._dataCols.push_back(t._dataCols[idx]);
    }

    v._dataCols.emplace_back(v._dataCols[0].size());
    v._dataCols.emplace_back(v._dataCols[0].size());

    return v;
}

View Views::nestedLoopJoin(View &v0, View &v1, std::string &field0, std::string &field1) {
    // remove padding and valid columns
    size_t effectiveFieldNum0 = v0.colNum() - 2;
    size_t effectiveFieldNum1 = v1.colNum() - 2;

    std::vector<std::string> fieldNames(effectiveFieldNum0 + effectiveFieldNum1);
    std::string tableName0 = v0._tableName.empty() ? "$t0" : v0._tableName;
    std::string tableName1 = v1._tableName.empty() ? "$t1" : v1._tableName;

    for (int i = 0; i < effectiveFieldNum0; ++i) {
        fieldNames[i] = tableName0 + "." + v0._fieldNames[i];
    }
    for (int i = 0; i < effectiveFieldNum1; ++i) {
        fieldNames[i + effectiveFieldNum0] = getAliasColName(tableName1, v1._fieldNames[i]);
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
                    &cmp0, &cmp1, width, 0, b * BoolEqualBatchOperator::msgTagCount(),
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

    joined.clearInvalidEntries();

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

    for (int layer = 0; layer < numLayers; layer++) {
        int stride = 1 << layer;

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

                    // Prepare routing bits for all elements
                    std::vector<int64_t> routingBits;
                    routingBits.reserve(totalRows);

                    for (size_t r = 0; r < rows1; r++) {
                        routingBits.push_back(Math::getBit(bucket1[tagColIndex][r], bitPosition));
                    }

                    for (size_t r = 0; r < rows2; r++) {
                        routingBits.push_back(Math::getBit(bucket2[tagColIndex][r], bitPosition));
                    }

                    std::vector<std::vector<int64_t> > newBucket1;
                    std::vector<std::vector<int64_t> > newBucket2;

                    for (int col = 0; col < bucket1.size(); col++) {
                        std::vector<int64_t> mergedData;
                        mergedData.reserve(totalRows);
                        if (!bucket1.empty()) {
                            mergedData.insert(mergedData.end(), bucket1[col].begin(), bucket1[col].end());
                        }
                        if (!bucket2.empty()) {
                            mergedData.insert(mergedData.end(), bucket2[col].begin(), bucket2[col].end());
                        }

                        std::vector<int64_t> dummyData(totalRows, 0);

                        auto bucket1Data =
                                BoolMutexBatchOperator(
                                    &mergedData, &dummyData, &routingBits, view._fieldWidths[col], 0,
                                    msgTagBase + layer * 1000 + col * 10).execute()->_zis;


                        size_t half = bucket1Data.size() / 2;
                        auto bucket2Data = std::vector(bucket1Data.begin() + half, bucket1Data.end());
                        bucket1Data.resize(half);

                        newBucket1.push_back(std::move(bucket1Data));
                        newBucket2.push_back(std::move(bucket2Data));
                    }

                    bucket1 = std::move(newBucket1);
                    bucket2 = std::move(newBucket2);
                }
            }
        }
    }

    return buckets;
}

// Perform nested loop joins on bucket pairs
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

        left.clearInvalidEntries();
        right.clearInvalidEntries();

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

// Main shuffle bucket join function
View Views::hashJoin(View &v0, View &v1, std::string &field0, std::string &field1) {
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

    auto buckets0 = butterflyPermutation(v0, tagColIndex0, 0);

    int msgTagBase1 = 10000; // Offset to avoid conflicts
    auto buckets1 = butterflyPermutation(v1, tagColIndex1, msgTagBase1);

    for (int i = 0; i < numBuckets; i++) {
        if (!buckets0[i].empty() && !buckets1[i].empty() &&
            !buckets0[i][0].empty() && !buckets1[i][0].empty()) {
        }
    }

    int msgTagBase2 = 20000; // Offset for join operations
    auto result = performBucketJoins(buckets0, buckets1, v0, v1, field0, field1);

    return result;
}

std::string Views::getAliasColName(std::string &tableName, std::string &fieldName) {
    return tableName + "." + fieldName;
}

void Views::addRedundantCols(View &v) {
    v._fieldNames.emplace_back(View::VALID_COL_NAME);
    v._fieldWidths.emplace_back(1);
    v._dataCols.emplace_back(v._dataCols[0].size());

    v._fieldNames.emplace_back(View::PADDING_COL_NAME);
    v._fieldWidths.emplace_back(1);
    v._dataCols.emplace_back(v._dataCols[0].size());
}
