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

View Views::selectAll(Table &t) {
    View v(t._tableName, t._fieldNames, t._fieldWidths);
    v._dataCols = t._dataCols;
    v._dataCols.emplace_back(t._dataCols[0].size(), Comm::rank());
    v._dataCols.emplace_back(t._dataCols[0].size());
    return v;
}

View Views::selectColumns(Table &t, std::vector<std::string> &fieldNames) {
    std::vector<size_t> indices;
    indices.reserve(fieldNames.size());
    for (const auto &name: fieldNames) {
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

                // std::vector<int> idx0, idx1;

                for (int i = 0; i < batchSize; ++i) {
                    int temp = b * batchSize + i;
                    if (temp / rows1 == rows0) {
                        break;
                    }
                    cmp0.push_back(col0[temp / rows1]);
                    cmp1.push_back(col1[temp % rows1]);
                    // idx0.push_back(temp / rows1);
                    // idx1.push_back(temp % rows1);
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

View Views::shuffleBucketJoin(View &v0, View &v1, std::string &field0, std::string &field1) {
    std::string tagField0, tagField1;
    int tagColIdx0 = v0.colIndex(Table::BUCKET_TAG_PREFIX + field0);
    if (tagColIdx0 == -1) {
        return nestedLoopJoin(v0, v1, field0, field1);
    }
    int tagColIdx1 = v1.colIndex(Table::BUCKET_TAG_PREFIX + field1);
    if (tagColIdx1 == -1) {
        return nestedLoopJoin(v0, v1, field0, field1);
    }

    // Get number of buckets from configuration
    int numBuckets = DbConf::SHUFFLE_BUCKET_NUM;

    // Distribute records to buckets based on tag shares
    auto buckets0 = distributeToBuckets(v0, tagColIdx0, numBuckets);
    auto buckets1 = distributeToBuckets(v1, tagColIdx1, numBuckets);

    // Perform nested loop join within each corresponding bucket
    return joinBuckets(v0, v1, buckets0, buckets1, field0, field1);
}

std::string Views::getAliasColName(std::string &tableName, std::string &fieldName) {
    return tableName + "." + fieldName;
}

std::vector<std::vector<size_t> > Views::distributeToBuckets(const View &v, int tagColIdx, int numBuckets) {
    std::vector<std::vector<size_t> > buckets(numBuckets);

    if (v._dataCols.empty() || tagColIdx == -1) {
        return buckets;
    }

    size_t numRows = v._dataCols[0].size();

    // For secret sharing, we need to use secure operations to determine bucket assignment
    // Since tag shares are secret, we can't directly use them for bucketing
    // We'll use secure equality comparison to assign records to buckets

    for (int bucketId = 0; bucketId < numBuckets; ++bucketId) {
        // Create a vector of bucket IDs for comparison
        std::vector<int64_t> bucketIdShares(numRows);
        for (size_t i = 0; i < numRows; ++i) {
            // For secret sharing, we need to create shares of the bucket ID
            // Here we use a simple approach: party 0 holds the bucket ID, party 1 holds 0
            bucketIdShares[i] = (Comm::rank() == 0) ? bucketId : 0;
        }

        // Get tag shares for all records
        std::vector<int64_t> tagShares(v._dataCols[tagColIdx].begin(),
                                       v._dataCols[tagColIdx].end());

        // Perform secure equality comparison to determine which records belong to this bucket
        auto equalityResults = BoolEqualBatchOperator(
            &tagShares, &bucketIdShares, v._fieldWidths[tagColIdx], 0,
            bucketId * BoolEqualBatchOperator::msgTagCount(),
            SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        // For now, we'll use a simplified approach where we add all records to all buckets
        // In a real implementation, we would use the equality results to filter
        // This is a placeholder that maintains the interface
        for (size_t i = 0; i < numRows; ++i) {
            buckets[bucketId].push_back(i);
        }
    }

    return buckets;
}

View Views::joinBuckets(View &v0, View &v1,
                        const std::vector<std::vector<size_t> > &buckets0,
                        const std::vector<std::vector<size_t> > &buckets1,
                        const std::string &field0, const std::string &field1) {
    // Create result view structure
    size_t fieldNum0 = v0._fieldNames.size();
    size_t fieldNum1 = v1._fieldNames.size();
    std::vector<std::string> fieldNames(fieldNum0 + fieldNum1);
    for (int i = 0; i < fieldNum0; ++i) {
        fieldNames[i] = v0._tableName + "." + v0._fieldNames[i];
    }
    for (int i = 0; i < fieldNum1; ++i) {
        fieldNames[i + fieldNum0] = v1._tableName + "." + v1._fieldNames[i];
    }

    std::vector<int> fieldWidths(fieldNum0 + fieldNum1);
    for (int i = 0; i < fieldNum0; ++i) {
        fieldWidths[i] = v0._fieldWidths[i];
    }
    for (int i = 0; i < fieldNum1; ++i) {
        fieldWidths[i + fieldNum0] = v1._fieldWidths[i];
    }

    View joined(fieldNames, fieldWidths);

    if (v0._dataCols.empty() || v1._dataCols.empty()) {
        return joined;
    }

    // Initialize result columns
    std::vector<std::vector<int64_t> > resultCols(fieldNum0 + fieldNum1 + 2);

    int colIndex0 = v0.colIndex(field0);
    int colIndex1 = v1.colIndex(field1);

    // Process each bucket pair
    int numBuckets = std::min(buckets0.size(), buckets1.size());

    for (int bucketId = 0; bucketId < numBuckets; ++bucketId) {
        const auto &bucket0 = buckets0[bucketId];
        const auto &bucket1 = buckets1[bucketId];

        if (bucket0.empty() || bucket1.empty()) {
            continue;
        }

        // Perform nested loop join within this bucket
        size_t bucketSize0 = bucket0.size();
        size_t bucketSize1 = bucket1.size();
        size_t bucketJoinSize = bucketSize0 * bucketSize1;

        if (bucketJoinSize == 0) continue;

        // Prepare comparison vectors for this bucket
        std::vector<int64_t> cmp0, cmp1;
        cmp0.reserve(bucketJoinSize);
        cmp1.reserve(bucketJoinSize);

        size_t startIdx = resultCols[0].size();

        // Resize result columns to accommodate new records
        for (auto &col: resultCols) {
            col.resize(startIdx + bucketJoinSize);
        }

        size_t resultIdx = startIdx;

        // Generate all pairs within this bucket
        for (size_t i = 0; i < bucketSize0; ++i) {
            size_t idx0 = bucket0[i];
            for (size_t j = 0; j < bucketSize1; ++j) {
                size_t idx1 = bucket1[j];

                // Copy data from v0
                for (size_t k = 0; k < fieldNum0; ++k) {
                    resultCols[k][resultIdx] = v0._dataCols[k][idx0];
                }

                // Copy data from v1
                for (size_t k = 0; k < fieldNum1; ++k) {
                    resultCols[k + fieldNum0][resultIdx] = v1._dataCols[k][idx1];
                }

                // Prepare for equality comparison
                cmp0.push_back(v0._dataCols[colIndex0][idx0]);
                cmp1.push_back(v1._dataCols[colIndex1][idx1]);

                resultIdx++;
            }
        }

        // Perform equality comparison for this bucket
        auto equalityResults = BoolEqualBatchOperator(
            &cmp0, &cmp1, v0._fieldWidths[colIndex0], 0,
            bucketId * BoolEqualBatchOperator::msgTagCount(),
            SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        // Store equality results
        for (size_t i = 0; i < bucketJoinSize; ++i) {
            resultCols[fieldNum0 + fieldNum1 + View::VALID_COL_OFFSET][startIdx + i] = equalityResults[i];
        }
    }

    // Set the result columns
    joined._dataCols = std::move(resultCols);

    return joined;
}

void Views::addRedundantCols(View &v) {
    v._fieldNames.emplace_back(View::VALID_COL_NAME);
    v._fieldWidths.emplace_back(1);
    v._dataCols.emplace_back(v._dataCols[0].size());

    v._fieldNames.emplace_back(View::PADDING_COL_NAME);
    v._fieldWidths.emplace_back(1);
    v._dataCols.emplace_back(v._dataCols[0].size());
}
