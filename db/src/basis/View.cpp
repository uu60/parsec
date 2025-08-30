//
// Created by 杜建璋 on 25-4-25.
//

#include "../../include/basis/View.h"

#include <numeric>

#include "compute/batch/bool/BoolAndBatchOperator.h"
#include "compute/batch/bool/BoolEqualBatchOperator.h"
#include "compute/batch/bool/BoolLessBatchOperator.h"
#include "compute/batch/bool/BoolMutexBatchOperator.h"
#include "compute/batch/bool/BoolToArithBatchOperator.h"
#include "compute/batch/arith/ArithMutexBatchOperator.h"
#include "compute/batch/arith/ArithMultiplyBatchOperator.h"
#include "parallel/ThreadPoolSupport.h"
#include "secret/Secrets.h"
#include "utils/Log.h"
#include "comm/Comm.h"

#include <string>

#include "compute/batch/arith/ArithToBoolBatchOperator.h"
#include "conf/DbConf.h"
#include "utils/StringUtils.h"

View::View(std::vector<std::string> &fieldNames,
           std::vector<int> &fieldWidths) : Table(
    EMPTY_VIEW_NAME, fieldNames, fieldWidths, EMPTY_KEY_FIELD) {
    // for padding column and valid column
    addRedundantCols();
}

View::View(std::string &tableName, std::vector<std::string> &fieldNames, std::vector<int> &fieldWidths) : Table(
    tableName, fieldNames, fieldWidths, EMPTY_KEY_FIELD) {
    addRedundantCols();
}

View::View(std::string &tableName, std::vector<std::string> &fieldNames, std::vector<int> &fieldWidths,
           bool dummy) : Table(
    tableName, fieldNames, fieldWidths, EMPTY_KEY_FIELD) {
}

void View::select(std::vector<std::string> &fieldNames) {
    if (fieldNames.empty()) {
        return;
    }

    // Get indices of selected fields
    std::vector<int> selectedIndices;
    selectedIndices.reserve(fieldNames.size());

    Log::i("fields: {} needed: {}", StringUtils::vecToString(_fieldNames), StringUtils::vecToString(fieldNames));
    for (const std::string &fieldName: fieldNames) {
        int index = colIndex(fieldName);
        if (index >= 0) {
            selectedIndices.push_back(index);
        }
    }

    if (selectedIndices.empty()) {
        return;
    }

    // Calculate indices for the last two columns (valid and padding)
    int totalCols = static_cast<int>(colNum());
    int validColIndex = totalCols + VALID_COL_OFFSET; // colNum() - 2
    int paddingColIndex = totalCols + PADDING_COL_OFFSET; // colNum() - 1

    // Create new data structures for the selected columns + last two columns
    std::vector<std::vector<int64_t> > newDataCols;
    std::vector<std::string> newFieldNames;
    std::vector<int> newFieldWidths;

    // Reserve space for selected columns + valid + padding columns
    size_t newSize = selectedIndices.size() + 2;
    newDataCols.reserve(newSize);
    newFieldNames.reserve(newSize);
    newFieldWidths.reserve(newSize);

    // Copy selected columns
    for (int index: selectedIndices) {
        newDataCols.push_back(std::move(_dataCols[index]));
        newFieldNames.push_back(std::move(_fieldNames[index]));
        newFieldWidths.push_back(_fieldWidths[index]);
    }

    // Copy the last two columns (valid and padding)
    newDataCols.push_back(std::move(_dataCols[validColIndex]));
    newDataCols.push_back(std::move(_dataCols[paddingColIndex]));
    newFieldNames.push_back(VALID_COL_NAME);
    newFieldNames.push_back(PADDING_COL_NAME);
    newFieldWidths.push_back(1); // valid column width
    newFieldWidths.push_back(1); // padding column width

    // Replace the current view's data with the new selected data
    _dataCols = std::move(newDataCols);
    _fieldNames = std::move(newFieldNames);
    _fieldWidths = std::move(newFieldWidths);
}

void View::sort(const std::string &orderField, bool ascendingOrder, int msgTagBase) {
    size_t n = rowNum();
    if (n == 0) {
        return;
    }
    bool isPowerOf2 = (n > 0) && ((n & (n - 1)) == 0);
    if (!isPowerOf2) {
        size_t nextPow2 = static_cast<size_t>(1) <<
                          static_cast<size_t>(std::ceil(std::log2(n)));
        // for each column, fill to next 2's pow with 1
        for (auto &v: _dataCols) {
            v.resize(nextPow2, 1);
        }
    }
    bitonicSort(orderField, ascendingOrder, msgTagBase);
    if (n < _dataCols[0].size()) {
        for (auto &v: _dataCols) {
            v.resize(n);
        }
    }
}

std::vector<int64_t> View::groupBySingleBatch(const std::string &groupField, int msgTagBase) {
    size_t n = rowNum();
    int groupFieldIndex = colIndex(groupField);
    auto &groupCol = _dataCols[groupFieldIndex];

    // Step 2: Identify group boundaries by comparing adjacent rows
    // Create a boundary indicator vector: 1 if current row starts a new group, 0 otherwise
    std::vector<int64_t> groupHeads(n);
    groupHeads[0] = Comm::rank(); // First row always starts a new group (Alice=1, Bob=0, XOR=1)

    if (n > 1) {
        std::vector<int64_t> currData, prevData;
        currData.reserve(n - 1);
        prevData.reserve(n - 1);

        for (size_t i = 1; i < n; i++) {
            currData.push_back(groupCol[i]);
            prevData.push_back(groupCol[i - 1]);
        }

        // Compare current row with previous row
        auto eqs = BoolEqualBatchOperator(&currData, &prevData, _fieldWidths[groupFieldIndex],
                                          0, msgTagBase,
                                          SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        // If equal to previous, not a boundary (0), if different, is a boundary (1)
        for (size_t i = 1; i < n; i++) {
            groupHeads[i] = eqs[i - 1] ^ Comm::rank(); // XOR with rank to flip: equal=0, different=1
        }
    }

    // // Remove all columns except the group field and the redundant columns (valid, padding)
    // std::vector<std::vector<int64_t> > newDataCols;
    // std::vector<std::string> newFieldNames;
    // std::vector<int> newFieldWidths;
    //
    // // Reserve space to prevent reallocation
    // newDataCols.reserve(3); // group field + valid + padding
    // newFieldNames.reserve(3);
    // newFieldWidths.reserve(3);
    //
    // // Move the group field data (avoid copy)
    // newDataCols.push_back(std::move(_dataCols[groupFieldIndex]));
    // newFieldNames.push_back(std::move(_fieldNames[groupFieldIndex]));
    // newFieldWidths.push_back(_fieldWidths[groupFieldIndex]);
    //
    // // Move the redundant columns (valid and padding)
    // newDataCols.push_back(std::move(_dataCols[colNum() + VALID_COL_OFFSET]));
    // newDataCols.push_back(std::move(_dataCols[colNum() + PADDING_COL_OFFSET]));
    // newFieldNames.push_back(VALID_COL_NAME);
    // newFieldNames.push_back(PADDING_COL_NAME);
    // newFieldWidths.push_back(1);
    // newFieldWidths.push_back(1);
    //
    // // Replace the current view's data
    // _dataCols = std::move(newDataCols);
    // _fieldNames = std::move(newFieldNames);
    // _fieldWidths = std::move(newFieldWidths);

    return groupHeads;
}

std::vector<int64_t> View::groupByMultiBatches(const std::string &groupField, int msgTagBase) {
    size_t n = rowNum();

    if (n <= Conf::BATCH_SIZE) {
        return groupBySingleBatch(groupField, msgTagBase);
    }

    int groupFieldIndex = colIndex(groupField);
    auto &groupCol = _dataCols[groupFieldIndex];

    // Step 2: Identify group boundaries by comparing adjacent rows
    std::vector<int64_t> groupHeads(n);
    groupHeads[0] = Comm::rank(); // First row always starts a new group

    if (n > 1) {
        int batchSize = Conf::BATCH_SIZE;
        int batchNum = ((n - 1) + batchSize - 1) / batchSize; // Number of batches for row comparisons
        int tagOffset = BoolEqualBatchOperator::tagStride();

        // Process comparisons in batches
        std::vector<std::future<std::vector<int64_t> > > batchFutures(batchNum);

        for (int b = 0; b < batchNum; ++b) {
            batchFutures[b] = ThreadPoolSupport::submit([&, b]() {
                int start = b * batchSize + 1; // +1 because we start from row 1
                int end = std::min(start + batchSize, static_cast<int>(n));
                int currentBatchSize = end - start;

                if (currentBatchSize <= 0) {
                    return std::vector<int64_t>();
                }

                std::vector<int64_t> currData, prevData;
                currData.reserve(currentBatchSize);
                prevData.reserve(currentBatchSize);

                for (int i = start; i < end; i++) {
                    currData.push_back(groupCol[i]);
                    prevData.push_back(groupCol[i - 1]);
                }

                int batchStartTag = msgTagBase + b * tagOffset;
                return BoolEqualBatchOperator(&currData, &prevData, _fieldWidths[groupFieldIndex],
                                              0, batchStartTag,
                                              SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
            });
        }

        // Collect results from all batches
        std::vector<int64_t> eqs;
        eqs.reserve(n - 1);
        for (auto &f: batchFutures) {
            auto batchResult = f.get();
            eqs.insert(eqs.end(), batchResult.begin(), batchResult.end());
        }

        // If equal to previous, not a boundary (0), if different, is a boundary (1)
        for (size_t i = 1; i < n; i++) {
            groupHeads[i] = eqs[i - 1] ^ Comm::rank(); // XOR with rank to flip: equal=0, different=1
        }
    }

    // // Remove all columns except the group field and the redundant columns (valid, padding)
    // std::vector<std::vector<int64_t> > newDataCols;
    // std::vector<std::string> newFieldNames;
    // std::vector<int> newFieldWidths;
    //
    // // Reserve space to prevent reallocation
    // newDataCols.reserve(3); // group field + valid + padding
    // newFieldNames.reserve(3);
    // newFieldWidths.reserve(3);
    //
    // // Move the group field data (avoid copy)
    // newDataCols.push_back(std::move(_dataCols[groupFieldIndex]));
    // newFieldNames.push_back(std::move(_fieldNames[groupFieldIndex]));
    // newFieldWidths.push_back(_fieldWidths[groupFieldIndex]);
    //
    // // Move the redundant columns (valid and padding)
    // newDataCols.push_back(std::move(_dataCols[colNum() + VALID_COL_OFFSET]));
    // newDataCols.push_back(std::move(_dataCols[colNum() + PADDING_COL_OFFSET]));
    // newFieldNames.push_back(VALID_COL_NAME);
    // newFieldNames.push_back(PADDING_COL_NAME);
    // newFieldWidths.push_back(1);
    // newFieldWidths.push_back(1);
    //
    // // Replace the current view's data
    // _dataCols = std::move(newDataCols);
    // _fieldNames = std::move(newFieldNames);
    // _fieldWidths = std::move(newFieldWidths);

    return groupHeads;
}

std::vector<int64_t> View::groupBySingleBatch(const std::vector<std::string> &groupFields, int msgTagBase) {
    size_t n = rowNum();

    // Get column indices for all group fields
    std::vector<int> groupFieldIndices(groupFields.size());
    for (size_t i = 0; i < groupFields.size(); i++) {
        groupFieldIndices[i] = colIndex(groupFields[i]);
    }

    // Step 3: Identify group boundaries by comparing adjacent rows across all columns
    std::vector<int64_t> groupHeads(n);
    groupHeads[0] = Comm::rank(); // First row always starts a new group (Alice=1, Bob=0, XOR=1)

    if (n > 1) {
        // Compare each row with the previous row across all group columns
        std::vector<int64_t> currData, prevData;
        currData.reserve((n - 1) * groupFields.size());
        prevData.reserve((n - 1) * groupFields.size());

        // Collect data for all columns and all adjacent row pairs
        for (size_t col = 0; col < groupFields.size(); col++) {
            int fieldIndex = groupFieldIndices[col];
            for (size_t i = 1; i < n; i++) {
                currData.push_back(_dataCols[fieldIndex][i]);
                prevData.push_back(_dataCols[fieldIndex][i - 1]);
            }
        }

        // Compare current row with previous row for first column
        std::vector<int64_t> firstColCurr(currData.begin(), currData.begin() + (n - 1));
        std::vector<int64_t> firstColPrev(prevData.begin(), prevData.begin() + (n - 1));

        auto eqs = BoolEqualBatchOperator(&firstColCurr, &firstColPrev,
                                          _fieldWidths[groupFieldIndices[0]],
                                          0, msgTagBase,
                                          SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        // For remaining columns, compute equality and combine with AND
        for (size_t col = 1; col < groupFields.size(); col++) {
            size_t startIdx = col * (n - 1);
            std::vector<int64_t> colCurr(currData.begin() + startIdx, currData.begin() + startIdx + (n - 1));
            std::vector<int64_t> colPrev(prevData.begin() + startIdx, prevData.begin() + startIdx + (n - 1));

            auto colEqs = BoolEqualBatchOperator(&colCurr, &colPrev,
                                                 _fieldWidths[groupFieldIndices[col]],
                                                 0, msgTagBase,
                                                 SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

            // Combine with previous equality results using AND
            eqs = BoolAndBatchOperator(&eqs, &colEqs, 1, 0, msgTagBase,
                                       SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        }

        // If all columns are equal to previous row, not a boundary (0)
        // If any column is different, is a boundary (1)
        for (size_t i = 1; i < n; i++) {
            groupHeads[i] = eqs[i - 1] ^ Comm::rank(); // XOR with rank to flip: equal=0, different=1
        }
    }

    return groupHeads;

    // // Remove all columns except the group fields and the redundant columns (valid, padding)
    // std::vector<std::vector<int64_t> > newDataCols;
    // std::vector<std::string> newFieldNames;
    // std::vector<int> newFieldWidths;
    //
    // // Reserve space to prevent reallocation
    // size_t totalCols = groupFields.size() + 2; // group fields + valid + padding
    // newDataCols.reserve(totalCols);
    // newFieldNames.reserve(totalCols);
    // newFieldWidths.reserve(totalCols);
    //
    // // Move the group field data (avoid copy)
    // for (size_t i = 0; i < groupFields.size(); i++) {
    //     int fieldIndex = groupFieldIndices[i];
    //     newDataCols.push_back(std::move(_dataCols[fieldIndex]));
    //     newFieldNames.push_back(std::move(_fieldNames[fieldIndex]));
    //     newFieldWidths.push_back(_fieldWidths[fieldIndex]);
    // }
    //
    // // Move the redundant columns (valid and padding)
    // newDataCols.push_back(std::move(_dataCols[colNum() + VALID_COL_OFFSET]));
    // newDataCols.push_back(std::move(_dataCols[colNum() + PADDING_COL_OFFSET]));
    // newFieldNames.push_back(VALID_COL_NAME);
    // newFieldNames.push_back(PADDING_COL_NAME);
    // newFieldWidths.push_back(1);
    // newFieldWidths.push_back(1);
    //
    // // Replace the current view's data
    // _dataCols = std::move(newDataCols);
    // _fieldNames = std::move(newFieldNames);
    // _fieldWidths = std::move(newFieldWidths);
    //
    // return groupHeads;
}

std::vector<int64_t> View::groupByMultiBatches(const std::vector<std::string> &groupFields, int msgTagBase) {
    size_t n = rowNum();
    if (n <= Conf::BATCH_SIZE) {
        return groupBySingleBatch(groupFields, msgTagBase);
    }

    // Get column indices for all group fields
    std::vector<int> groupFieldIndices(groupFields.size());
    for (size_t i = 0; i < groupFields.size(); i++) {
        groupFieldIndices[i] = colIndex(groupFields[i]);
    }

    // Step 3: Identify group boundaries by comparing adjacent rows across all columns
    std::vector<int64_t> groupHeads(n);
    groupHeads[0] = Comm::rank(); // First row always starts a new group

    if (n > 1) {
        int batchSize = Conf::BATCH_SIZE;
        int batchNum = ((n - 1) + batchSize - 1) / batchSize; // Number of batches for row comparisons
        int tagOffset = std::max(BoolEqualBatchOperator::tagStride(), BoolAndBatchOperator::tagStride());

        // Process each column in parallel
        std::vector<std::future<std::vector<int64_t> > > columnFutures(groupFields.size());

        for (int colIdx = 0; colIdx < groupFields.size(); colIdx++) {
            columnFutures[colIdx] = ThreadPoolSupport::submit([&, colIdx] {
                int fieldIndex = groupFieldIndices[colIdx];
                std::vector<int64_t> columnResult;
                columnResult.reserve(n - 1);

                // Process this column in batches
                std::vector<std::future<std::vector<int64_t> > > batchFutures(batchNum);
                int startTag = tagOffset * batchNum * colIdx;

                for (int b = 0; b < batchNum; ++b) {
                    batchFutures[b] = ThreadPoolSupport::submit([&, b, fieldIndex, startTag]() {
                        int start = b * batchSize + 1; // +1 because we start from row 1
                        int end = std::min(start + batchSize, static_cast<int>(n));
                        int currentBatchSize = end - start;

                        if (currentBatchSize <= 0) {
                            return std::vector<int64_t>();
                        }

                        std::vector<int64_t> currData, prevData;
                        currData.reserve(currentBatchSize);
                        prevData.reserve(currentBatchSize);

                        for (int i = start; i < end; i++) {
                            currData.push_back(_dataCols[fieldIndex][i]);
                            prevData.push_back(_dataCols[fieldIndex][i - 1]);
                        }

                        int batchStartTag = startTag + b * tagOffset;
                        return BoolEqualBatchOperator(&currData, &prevData, _fieldWidths[fieldIndex],
                                                      0, batchStartTag,
                                                      SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
                    });
                }

                // Collect results from all batches for this column
                for (auto &f: batchFutures) {
                    auto batchResult = f.get();
                    columnResult.insert(columnResult.end(), batchResult.begin(), batchResult.end());
                }

                return columnResult;
            });
        }

        // Collect results from all columns
        std::vector<std::vector<int64_t> > columnResults(groupFields.size());
        for (int colIdx = 0; colIdx < groupFields.size(); colIdx++) {
            columnResults[colIdx] = columnFutures[colIdx].get();
        }

        // Combine results using AND operation
        std::vector<int64_t> eqs;
        if (!columnResults.empty()) {
            eqs = std::move(columnResults[0]);

            for (int colIdx = 1; colIdx < columnResults.size(); colIdx++) {
                eqs = BoolAndBatchOperator(&eqs, &columnResults[colIdx], 1, 0, msgTagBase,
                                           SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
            }
        }

        // If all columns are equal to previous row, not a boundary (0)
        // If any column is different, is a boundary (1)
        for (size_t i = 1; i < n; i++) {
            groupHeads[i] = eqs[i - 1] ^ Comm::rank(); // XOR with rank to flip: equal=0, different=1
        }
    }

    return groupHeads;
    // // Remove all columns except the group fields and the redundant columns (valid, padding)
    // std::vector<std::vector<int64_t> > newDataCols;
    // std::vector<std::string> newFieldNames;
    // std::vector<int> newFieldWidths;
    //
    // // Reserve space to prevent reallocation
    // size_t totalCols = groupFields.size() + 2; // group fields + valid + padding
    // newDataCols.reserve(totalCols);
    // newFieldNames.reserve(totalCols);
    // newFieldWidths.reserve(totalCols);
    //
    // // Move the group field data (avoid copy)
    // for (size_t i = 0; i < groupFields.size(); i++) {
    //     int fieldIndex = groupFieldIndices[i];
    //     newDataCols.push_back(std::move(_dataCols[fieldIndex]));
    //     newFieldNames.push_back(std::move(_fieldNames[fieldIndex]));
    //     newFieldWidths.push_back(_fieldWidths[fieldIndex]);
    // }
    //
    // // Move the redundant columns (valid and padding)
    // newDataCols.push_back(std::move(_dataCols[colNum() + VALID_COL_OFFSET]));
    // newDataCols.push_back(std::move(_dataCols[colNum() + PADDING_COL_OFFSET]));
    // newFieldNames.push_back(VALID_COL_NAME);
    // newFieldNames.push_back(PADDING_COL_NAME);
    // newFieldWidths.push_back(1);
    // newFieldWidths.push_back(1);
    //
    // // Replace the current view's data
    // _dataCols = std::move(newDataCols);
    // _fieldNames = std::move(newFieldNames);
    // _fieldWidths = std::move(newFieldWidths);
    //
    // return groupHeads;
}

void View::countSingleBatch(std::vector<int64_t> &heads, std::string alias, int msgTagBase) {
    const size_t n = rowNum();
    if (n == 0) return;

    // 组头（布尔分享）
    std::vector<int64_t> bs_bool = heads;

    // 是否不压缩
    const bool NO_COMPACTION = DbConf::DISABLE_COMPACTION;

    // 计数工作列（算术域）
    std::vector<int64_t> vs;

    if (NO_COMPACTION) {
        // 不压缩：只统计当前 valid=1 的行
        int validIdx = colNum() + VALID_COL_OFFSET;
        auto valid_bool = _dataCols[validIdx];
        vs = BoolToArithBatchOperator(&valid_bool, 64, 0, msgTagBase,
                                      SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    } else {
        // 压缩：原逻辑，所有行计为1（1-share用 Comm::rank()）
        vs.assign(n, Comm::rank());
    }

    // b 的算术份额（用于 (1 - a(b)) 门控）
    std::vector<int64_t> bs_arith =
        BoolToArithBatchOperator(&bs_bool, 64, 0, msgTagBase,
                                 SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

    // (v,b) ⊕ (v',b') = ( v' + (1 - a(b')) * v ,  b OR b' )
    // 这里 v' 是右半段 vs[i+delta]，v 是左半段 vs[i]
    for (int delta = 1; delta < (int)n; delta <<= 1) {
        const int m = (int)n - delta;

        // 计算右侧 (1 - a(b[i+delta])) * 左侧 v[i]
        std::vector<int64_t> mul_left(m), mul_right(m);
        for (int i = 0; i < m; ++i) {
            mul_left[i]  = Comm::rank() - bs_arith[i + delta]; // 1 - a(b')
            mul_right[i] = vs[i];                               // v
        }
        auto add_term = ArithMultiplyBatchOperator(&mul_left, &mul_right, 64, 0, msgTagBase,
                                                   SecureOperator::NO_CLIENT_COMPUTE)
                            .execute()->_zis;

        // 累加到右侧
        for (int i = 0; i < m; ++i) {
            vs[i + delta] += add_term[i];
        }

        // 传播 OR：b[i+delta] = b[i] OR b[i+delta] = ~(~b[i] & ~b[i+delta])
        std::vector<int64_t> not_l(m), not_r(m);
        for (int i = 0; i < m; ++i) {
            not_l[i] = bs_bool[i] ^ Comm::rank();
            not_r[i] = bs_bool[i + delta] ^ Comm::rank();
        }
        auto and_out = BoolAndBatchOperator(&not_l, &not_r, 1, 0, msgTagBase,
                                            SecureOperator::NO_CLIENT_COMPUTE)
                           .execute()->_zis;
        for (int i = 0; i < m; ++i) {
            bs_bool[i + delta] = and_out[i] ^ Comm::rank();
        }

        // 继续用最新的 b 计算 a(b)
        bs_arith = BoolToArithBatchOperator(&bs_bool, 64, 0, msgTagBase,
                                            SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    }

    // 算术 -> 布尔（输出计数）
    auto vs_bool = ArithToBoolBatchOperator(&vs, 64, 0, msgTagBase,
                                            SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

    // 组尾：tail[i] = head[i+1]；最后一行 = 1_share
    std::vector<int64_t> group_tails(n);
    for (size_t i = 0; i + 1 < n; ++i) group_tails[i] = heads[i + 1];
    group_tails[n - 1] = Comm::rank();

    // 插入计数字段（在 VALID 之前）
    _fieldNames.reserve(_fieldNames.size() + 1);
    _fieldWidths.reserve(_fieldWidths.size() + 1);
    _dataCols.reserve(_dataCols.size() + 1);
    const int insertPos = colNum() - 2;
    _fieldNames.insert(_fieldNames.begin() + insertPos, alias.empty() ? COUNT_COL_NAME : alias);
    _fieldWidths.insert(_fieldWidths.begin() + insertPos, 64);
    _dataCols.insert(_dataCols.begin() + insertPos, std::move(vs_bool));

    // 更新 VALID
    int validIdx = colNum() + VALID_COL_OFFSET;
    if (NO_COMPACTION) {
        // 不压缩：valid := valid ∧ group_tails
        auto new_valid = BoolAndBatchOperator(&_dataCols[validIdx], &group_tails, 1, 0, msgTagBase,
                                              SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        _dataCols[validIdx] = std::move(new_valid);
        // 不物理删除：保留整表，依赖 valid 逐步收敛
    } else {
        // 压缩：只保留组尾作为输出行
        _dataCols[validIdx] = std::move(group_tails);
        clearInvalidEntries(msgTagBase);
    }
}

void View::countMultiBatches(std::vector<int64_t> &heads, std::string alias, int msgTagBase) {
    size_t n = rowNum();
    const int batchSize = Conf::BATCH_SIZE;
    if (n == 0) return;
    if (n <= static_cast<size_t>(batchSize)) {
        countSingleBatch(heads, alias, msgTagBase);
        return;
    }

    const int64_t rank = Comm::rank();

    // 初值：计数份额 vs = 1_share，边界位 b = heads（XOR 份额）
    std::vector<int64_t> vs(n, rank);
    std::vector<int64_t> bs_bool = heads;

    // a(b) 供第一轮 delta 使用
    auto init_ab = BoolToArithBatchOperator(&bs_bool, 64, 0, msgTagBase,
                                            SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    std::vector<int64_t> bs_arith = std::move(init_ab);

    // 各算子 stride
    const int mulStride = ArithMultiplyBatchOperator::tagStride(64);
    const int andStride = BoolAndBatchOperator::tagStride();
    const int b2aStride = BoolToArithBatchOperator::tagStride();
    const int a2bStride = ArithToBoolBatchOperator::tagStride(64);
    const int taskStride = mulStride + andStride + b2aStride;

    int tagCursorBase = msgTagBase;

    for (int delta = 1; delta < static_cast<int>(n); delta <<= 1) {
        const int totalPairs = static_cast<int>(n) - delta;
        if (totalPairs <= 0) break;

        const int numBatches = (totalPairs + batchSize - 1) / batchSize;

        using Triple = std::tuple<std::vector<int64_t>, // temp_vs (len)
            std::vector<int64_t>, // new_bs_bool (len) for [start+delta .. end-1+delta]
            std::vector<int64_t> >; // new_bs_arith (len) same range

        std::vector<std::future<Triple> > futs(numBatches);

        for (int b = 0; b < numBatches; ++b) {
            const int start = b * batchSize;
            const int end = std::min(start + batchSize, totalPairs);
            const int len = end - start;

            const int baseTag = tagCursorBase + b * taskStride;
            const int mulTag = baseTag;
            const int andTag = baseTag + mulStride;
            const int b2aTag = baseTag + mulStride + andStride;

            futs[b] = ThreadPoolSupport::submit([=, &vs, &bs_bool, &bs_arith]() -> Triple {
                if (len <= 0) return Triple{{}, {}, {}};

                // 1) 乘法：temp_vs = (1 - a(b[i+delta])) * vs[i]
                std::vector<int64_t> mul_left(len), mul_right(len);
                for (int i = 0; i < len; ++i) {
                    const int idx = start + i;
                    mul_left[i] = rank - bs_arith[idx + delta];
                    mul_right[i] = vs[idx];
                }
                auto temp_vs = ArithMultiplyBatchOperator(&mul_left, &mul_right, 64, 0, mulTag,
                                                          SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

                // 2) OR：b[i+delta] = b[i] OR b[i+delta]
                // 使用 (~b[i]) AND (~b[i+delta]) 再取反
                std::vector<int64_t> not_left(len), not_right(len);
                for (int i = 0; i < len; ++i) {
                    const int idx = start + i;
                    not_left[i] = bs_bool[idx] ^ rank;
                    not_right[i] = bs_bool[idx + delta] ^ rank;
                }
                auto and_res = BoolAndBatchOperator(&not_left, &not_right, 1, 0, andTag,
                                                    SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

                std::vector<int64_t> new_bs_bool(len);
                for (int i = 0; i < len; ++i) {
                    new_bs_bool[i] = and_res[i] ^ rank; // 取反
                }

                // 3) 只把更新过的区间 [start+delta .. end-1+delta] 做 b->a，供下一轮 delta 使用
                auto new_bs_arith = BoolToArithBatchOperator(&new_bs_bool, 64, 0, b2aTag,
                                                             SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

                return Triple{std::move(temp_vs), std::move(new_bs_bool), std::move(new_bs_arith)};
            });
        }

        // 消费这一波任务的 tag 空间
        tagCursorBase += numBatches * taskStride;

        // 回填：vs[i+delta]、bs_bool[i+delta]、bs_arith[i+delta]
        for (int b = 0; b < numBatches; ++b) {
            const int start = b * batchSize;
            const int end = std::min(start + batchSize, totalPairs);
            const int len = end - start;
            if (len <= 0) continue;

            auto triple = futs[b].get();
            auto &temp_vs = std::get<0>(triple);
            auto &new_b_bool = std::get<1>(triple);
            auto &new_b_arith = std::get<2>(triple);

            // vs[i+delta] += temp_vs[i]
            for (int i = 0; i < len; ++i) {
                vs[start + i + delta] += temp_vs[i];
            }
            // b / a(b) 更新到 i+delta 位置
            for (int i = 0; i < len; ++i) {
                bs_bool[start + i + delta] = new_b_bool[i];
                bs_arith[start + i + delta] = new_b_arith[i];
            }
        }
    }

    // 末尾把 vs 从算术转成布尔（并行一波）
    {
        const int numBatches = (static_cast<int>(n) + batchSize - 1) / batchSize;
        std::vector<std::future<std::vector<int64_t> > > futs(numBatches);
        for (int b = 0; b < numBatches; ++b) {
            const int start = b * batchSize;
            const int end = std::min(start + batchSize, static_cast<int>(n));
            const int len = end - start;
            if (len <= 0) {
                futs[b] = ThreadPoolSupport::submit([=]() { return std::vector<int64_t>(); });
                continue;
            }
            auto slice = std::make_shared<std::vector<int64_t> >(vs.begin() + start, vs.begin() + end);
            const int a2bTag = tagCursorBase + b * a2bStride;
            futs[b] = ThreadPoolSupport::submit([slice, a2bTag]() {
                return ArithToBoolBatchOperator(slice.get(), 64, 0, a2bTag,
                                                SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
            });
        }
        tagCursorBase += numBatches * a2bStride;

        int pos = 0;
        for (int b = 0; b < numBatches; ++b) {
            auto part = futs[b].get();
            for (auto v: part) {
                if (pos < static_cast<int>(n)) vs[pos++] = v;
            }
        }
    }

    // 跟单批版一致：构造 group_tails / 插入列 / 更新 VALID / 清理
    std::vector<int64_t> group_tails(heads.size());
    if (!heads.empty()) {
        for (int i = 0; i < static_cast<int>(heads.size()) - 1; ++i) {
            group_tails[i] = heads[i + 1];
        }
        group_tails.back() = rank;
    }

    _fieldNames.reserve(_fieldNames.size() + 1);
    _fieldWidths.reserve(_fieldWidths.size() + 1);
    _dataCols.reserve(_dataCols.size() + 1);

    _fieldNames.insert(_fieldNames.begin() + colNum() - 2, 1, alias.empty() ? COUNT_COL_NAME : alias);
    _fieldWidths.insert(_fieldWidths.begin() + colNum() - 2, 1, 64);
    _dataCols.insert(_dataCols.begin() + colNum() - 2, 1, vs);

    _dataCols[colNum() + VALID_COL_OFFSET] = std::move(group_tails);
    clearInvalidEntries(msgTagBase);
}

void View::maxSingleBatch(std::vector<int64_t> &heads, const std::string &fieldName, std::string alias,
                          int msgTagBase) {
    const size_t n = rowNum();
    if (n == 0) return;

    // —— 基础检查与获取列信息
    const int fieldIdx = colIndex(fieldName);
    if (fieldIdx < 0) {
        Log::e("Field '{}' not found for max operation", fieldName);
        return;
    }
    const int bitlen = _fieldWidths[fieldIdx];
    const bool NO_COMPACTION = DbConf::DISABLE_COMPACTION;

    std::vector<int64_t> &src = _dataCols[fieldIdx];
    if (src.size() != n || heads.size() != n) {
        Log::e("Size mismatch: fieldData={}, heads={}, n={}", src.size(), heads.size(), n);
        return;
    }

    // —— 工作副本：值列与组首布尔标记
    std::vector<int64_t> vs = src;          // 值工作列
    std::vector<int64_t> bs_bool = heads;   // 组头(XOR布尔分享)
    const int64_t NOT_mask = Comm::rank();  // 单侧取反：NOT(x)=x^mask

    // 当前 valid（布尔分享），用于不压缩分支的掩蔽
    const int validIdx = colNum() + VALID_COL_OFFSET;
    std::vector<int64_t> valid_bool;
    if (NO_COMPACTION) {
        valid_bool = _dataCols[validIdx];

        // 先对整列值做一次全局掩蔽：vs = valid ? vs : 0
        std::vector<int64_t> zeros(n, 0);
        vs = BoolMutexBatchOperator(&vs, &zeros, &valid_bool, bitlen, /*tid*/0, msgTagBase,
                                    SecureOperator::NO_CLIENT_COMPUTE)
                 .execute()->_zis;
    }

    // —— 分段 max：Hillis–Steele 前缀扫描
    for (int delta = 1; delta < static_cast<int>(n); delta <<= 1) {
        const int m = static_cast<int>(n) - delta;

        // 1) 快照左右值
        std::vector<int64_t> left_vals(m), right_vals(m);
        for (int i = 0; i < m; ++i) {
            left_vals[i]  = vs[i];
            right_vals[i] = vs[i + delta];
        }

        // 不压缩：对每一轮参与的左右窗口再按各自 valid 切片做掩蔽，避免无效行干扰
        if (NO_COMPACTION) {
            std::vector<int64_t> zeros(m, 0);
            std::vector<int64_t> left_valid(m), right_valid(m);
            for (int i = 0; i < m; ++i) {
                left_valid[i]  = valid_bool[i];
                right_valid[i] = valid_bool[i + delta];
            }
            left_vals = BoolMutexBatchOperator(&left_vals, &zeros, &left_valid, bitlen, 0, msgTagBase,
                                               SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
            right_vals = BoolMutexBatchOperator(&right_vals, &zeros, &right_valid, bitlen, 0, msgTagBase,
                                                SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        }

        // 2) cand = max(left, right)  —— cmp=(left<right)，cand=cmp?right:left
        auto cmp = BoolLessBatchOperator(&left_vals, &right_vals, bitlen,
                                         /*tid*/0, msgTagBase,
                                         SecureOperator::NO_CLIENT_COMPUTE)
                        .execute()->_zis;

        auto cand = BoolMutexBatchOperator(&right_vals, &left_vals, &cmp,
                                           bitlen, /*tid*/0, msgTagBase,
                                           SecureOperator::NO_CLIENT_COMPUTE)
                        .execute()->_zis;

        // 3) 门控：gate = NOT(b[i+delta])，new = gate ? cand : right
        std::vector<int64_t> gate(m);
        for (int i = 0; i < m; ++i) {
            gate[i] = bs_bool[i + delta] ^ NOT_mask;
        }

        auto new_right = BoolMutexBatchOperator(&cand, &right_vals, &gate,
                                                bitlen, /*tid*/0, msgTagBase,
                                                SecureOperator::NO_CLIENT_COMPUTE)
                             .execute()->_zis;

        // 写回 v[i+delta]
        for (int i = 0; i < m; ++i) {
            vs[i + delta] = new_right[i];
        }

        // 4) 传播边界：b[i+delta] = b[i] OR b[i+delta] = ~(~b[i] & ~b[i+delta])
        std::vector<int64_t> not_l(m), not_r(m);
        for (int i = 0; i < m; ++i) {
            not_l[i] = bs_bool[i] ^ NOT_mask;         // NOT b[i]
            not_r[i] = bs_bool[i + delta] ^ NOT_mask; // NOT b[i+delta]
        }
        auto and_out = BoolAndBatchOperator(&not_l, &not_r,
                                            /*bitlen=*/1, /*tid*/0, msgTagBase,
                                            SecureOperator::NO_CLIENT_COMPUTE)
                           .execute()->_zis;

        for (int i = 0; i < m; ++i) {
            bs_bool[i + delta] = and_out[i] ^ NOT_mask; // NOT(AND) = OR
        }
    }

    // —— 组尾标记：tail[i] = head[i+1]，最后一行设为常量 1（按分享约定）
    std::vector<int64_t> group_tails(n);
    for (size_t i = 0; i + 1 < n; ++i) group_tails[i] = heads[i + 1];
    group_tails[n - 1] = Comm::rank();

    // —— 把 max 结果列插入（命名、宽度与原列一致）
    const std::string outName = alias.empty() ? ("max_" + fieldName) : alias;

    _fieldNames.reserve(_fieldNames.size() + 1);
    _fieldWidths.reserve(_fieldWidths.size() + 1);
    _dataCols.reserve(_dataCols.size() + 1);

    const int insertPos = colNum() - 2; // 在 VALID/PADDING 之前插入
    _fieldNames.insert(_fieldNames.begin() + insertPos, outName);
    _fieldWidths.insert(_fieldWidths.begin() + insertPos, bitlen);
    _dataCols.insert(_dataCols.begin() + insertPos, vs);

    // —— 更新有效位
    if (NO_COMPACTION) {
        // 不压缩：valid := valid ∧ group_tails（只在组尾行上“有效输出”）
        auto new_valid = BoolAndBatchOperator(&_dataCols[validIdx], &group_tails, 1, 0, msgTagBase,
                                              SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        _dataCols[validIdx] = std::move(new_valid);
        // 不物理删除：保留整表，依赖 valid 进行下游筛选
    } else {
        // 压缩：只保留组尾作为输出行
        _dataCols[validIdx] = std::move(group_tails);
        clearInvalidEntries(msgTagBase);
    }
}

void View::maxMultiBatches(std::vector<int64_t> &heads,
                           const std::string &fieldName,
                           std::string alias,
                           int msgTagBase) {
    const size_t n = rowNum();
    if (n == 0) return;

    const int fieldIdx = colIndex(fieldName);
    if (fieldIdx < 0) {
        Log::e("Field '{}' not found for max operation", fieldName);
        return;
    }
    const int bitlen = _fieldWidths[fieldIdx];

    auto &src = _dataCols[fieldIdx];
    if (src.size() != n || heads.size() != n) {
        Log::e("Size mismatch: fieldData={}, heads={}, n={}", src.size(), heads.size(), n);
        return;
    }

    const int batchSize = Conf::BATCH_SIZE;
    if (n <= static_cast<size_t>(batchSize)) {
        // 小数据量直接用单批实现
        maxSingleBatch(heads, fieldName, alias, msgTagBase);
        return;
    }

    // 工作副本：值列与组首布尔标记（XOR 份额）
    std::vector<int64_t> vs = src;
    std::vector<int64_t> bs_bool = heads;

    // 各算子 stride
    const int lessStride = BoolLessBatchOperator::tagStride();
    const int mutexStride = BoolMutexBatchOperator::tagStride();
    const int andStride = BoolAndBatchOperator::tagStride();
    // 每个批任务用到：Less(1) + Mutex(1) + Mutex(1) + AND(1)
    const int taskStride = lessStride + mutexStride + mutexStride + andStride;

    int tagCursorBase = msgTagBase;
    const int64_t NOT_mask = Comm::rank();

    // Hillis–Steele 前缀样式：delta = 1, 2, 4, ...
    for (int delta = 1; delta < static_cast<int>(n); delta <<= 1) {
        const int totalPairs = static_cast<int>(n) - delta;
        if (totalPairs <= 0) break;

        const int numBatches = (totalPairs + batchSize - 1) / batchSize;

        using Pair = std::pair<std::vector<int64_t>, std::vector<int64_t> >; // {new_right, new_b}

        std::vector<std::future<Pair> > futs(numBatches);

        for (int b = 0; b < numBatches; ++b) {
            const int start = b * batchSize;
            const int end = std::min(start + batchSize, totalPairs);
            const int len = end - start;

            const int baseTag = tagCursorBase + b * taskStride;
            const int lessTag = baseTag;
            const int mtx1Tag = baseTag + lessStride;
            const int mtx2Tag = mtx1Tag + mutexStride;
            const int andTag = mtx2Tag + mutexStride;

            futs[b] = ThreadPoolSupport::submit([=, &vs, &bs_bool]() -> Pair {
                if (len <= 0) return Pair{{}, {}};

                // 快照左右值 & 组头取反（作为门控 / 德摩根）
                std::vector<int64_t> left_vals(len), right_vals(len);
                std::vector<int64_t> not_bL(len), not_bR(len);
                for (int i = 0; i < len; ++i) {
                    const int idxL = start + i;
                    const int idxR = idxL + delta;
                    left_vals[i] = vs[idxL];
                    right_vals[i] = vs[idxR];
                    not_bL[i] = bs_bool[idxL] ^ NOT_mask; // ~b[i]
                    not_bR[i] = bs_bool[idxR] ^ NOT_mask; // ~b[i+delta]
                }

                // 1) cmp = (left < right)
                auto cmp = BoolLessBatchOperator(&left_vals, &right_vals, bitlen,
                                                 /*tid*/0, /*tag*/lessTag,
                                                 SecureOperator::NO_CLIENT_COMPUTE)
                        .execute()->_zis;

                // 2) cand = cmp ? right : left
                auto cand = BoolMutexBatchOperator(&right_vals, &left_vals, &cmp,
                                                   bitlen, /*tid*/0, /*tag*/mtx1Tag,
                                                   SecureOperator::NO_CLIENT_COMPUTE)
                        .execute()->_zis;

                // 3) gate = ~b[i+delta]；new_right = gate ? cand : right
                auto new_right = BoolMutexBatchOperator(&cand, &right_vals, &not_bR,
                                                        bitlen, /*tid*/0, /*tag*/mtx2Tag,
                                                        SecureOperator::NO_CLIENT_COMPUTE)
                        .execute()->_zis;

                // 4) 边界传播：b[i+delta] = b[i] OR b[i+delta] = ~(~b[i] & ~b[i+delta])
                auto and_out = BoolAndBatchOperator(&not_bL, &not_bR,
                                                    /*bitlen=*/1, /*tid*/0, /*tag*/andTag,
                                                    SecureOperator::NO_CLIENT_COMPUTE)
                        .execute()->_zis;
                std::vector<int64_t> new_b(len);
                for (int i = 0; i < len; ++i) new_b[i] = and_out[i] ^ NOT_mask;

                return Pair{std::move(new_right), std::move(new_b)};
            });
        }

        // 本轮 delta 消耗的 tag 空间
        tagCursorBase += numBatches * taskStride;

        // 写回右端：v[i+delta] 与 b[i+delta]
        for (int b = 0; b < numBatches; ++b) {
            const int start = b * batchSize;
            const int end = std::min(start + batchSize, totalPairs);
            const int len = end - start;
            if (len <= 0) continue;

            auto pr = futs[b].get();
            auto &new_right = pr.first;
            auto &new_b = pr.second;

            for (int i = 0; i < len; ++i) {
                const int idxR = start + i + delta;
                vs[idxR] = new_right[i];
                bs_bool[idxR] = new_b[i];
            }
        }
    }

    // 组尾标记：tail[i] = head[i+1]，最后一行 = 1_share
    std::vector<int64_t> group_tails(n);
    for (size_t i = 0; i + 1 < n; ++i) group_tails[i] = heads[i + 1];
    group_tails[n - 1] = Comm::rank();

    // 插入结果列（位置与单批版一致：在 VALID 之前）
    const std::string outName = alias.empty() ? ("max_" + fieldName) : alias;

    _fieldNames.reserve(_fieldNames.size() + 1);
    _fieldWidths.reserve(_fieldWidths.size() + 1);
    _dataCols.reserve(_dataCols.size() + 1);

    const int insertPos = colNum() - 2;
    _fieldNames.insert(_fieldNames.begin() + insertPos, outName);
    _fieldWidths.insert(_fieldWidths.begin() + insertPos, bitlen);
    _dataCols.insert(_dataCols.begin() + insertPos, std::move(vs));

    // 更新 VALID 为组尾，只保留每组一行（尾行）
    _dataCols[colNum() + VALID_COL_OFFSET] = std::move(group_tails);
    clearInvalidEntries(msgTagBase);
}

// —— 单批：分段最小值（Hillis–Steele 扫描 + 组边界传播）
void View::minSingleBatch(std::vector<int64_t> &heads,
                          const std::string &fieldName,
                          std::string alias,
                          int msgTagBase) {
    const size_t n = rowNum();
    if (n == 0) return;

    const int fieldIdx = colIndex(fieldName);
    if (fieldIdx < 0) {
        Log::e("Field '{}' not found for min operation", fieldName);
        return;
    }
    const int bitlen = _fieldWidths[fieldIdx];
    const bool NO_COMPACTION = DbConf::DISABLE_COMPACTION;

    std::vector<int64_t> &src = _dataCols[fieldIdx];
    if (src.size() != n || heads.size() != n) {
        Log::e("Size mismatch: fieldData={}, heads={}, n={}", src.size(), heads.size(), n);
        return;
    }

    std::vector<int64_t> vs = src;          // 值工作副本
    std::vector<int64_t> bs_bool = heads;   // 组头 (XOR)
    const int64_t NOT_mask = Comm::rank();

    // 不压缩模式下需要读取当前 valid（布尔分享）
    const int validIdx = colNum() + VALID_COL_OFFSET;
    std::vector<int64_t> valid_bool;
    if (NO_COMPACTION) {
        valid_bool = _dataCols[validIdx];
        // 注意：对于最小值，不能把无效行的值掩蔽为 0（会错误地成为最小值）。
        // 我们改为“只在右侧有效时才允许写回”，并用 left_valid 调整比较条件。
    }

    for (int delta = 1; delta < static_cast<int>(n); delta <<= 1) {
        const int m = static_cast<int>(n) - delta;

        // 1) 快照左右
        std::vector<int64_t> left_vals(m), right_vals(m);
        for (int i = 0; i < m; ++i) {
            left_vals[i]  = vs[i];
            right_vals[i] = vs[i + delta];
        }

        // 2) cmp_raw = (left < right)
        auto cmp_raw = BoolLessBatchOperator(&left_vals, &right_vals, bitlen,
                                             /*tid*/0, msgTagBase,
                                             SecureOperator::NO_CLIENT_COMPUTE)
                           .execute()->_zis;

        // 3) 根据压缩模式构造有效性门控
        //    - cmp_eff = (left_valid ∧ (left<right))；左无效⇒不选左（cand=右）
        //    - gate    = (~b[i+delta]) ∧ right_valid；右无效⇒不写回
        std::vector<int64_t> cmp_eff = cmp_raw;
        std::vector<int64_t> gate(m);

        if (NO_COMPACTION) {
            std::vector<int64_t> left_valid(m), right_valid(m);
            for (int i = 0; i < m; ++i) {
                left_valid[i]  = valid_bool[i];
                right_valid[i] = valid_bool[i + delta];
            }

            // cmp_eff = left_valid AND cmp_raw
            cmp_eff = BoolAndBatchOperator(&left_valid, &cmp_raw, 1, 0, msgTagBase,
                                           SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

            // gate_base = ~b[i+delta]
            std::vector<int64_t> gate_base(m);
            for (int i = 0; i < m; ++i) gate_base[i] = bs_bool[i + delta] ^ NOT_mask;

            // gate = gate_base AND right_valid
            gate = BoolAndBatchOperator(&gate_base, &right_valid, 1, 0, msgTagBase,
                                        SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        } else {
            // 原逻辑：只用组边界作为门控
            for (int i = 0; i < m; ++i) gate[i] = bs_bool[i + delta] ^ NOT_mask;
        }

        // 4) cand = min(left, right) = (cmp_eff ? left : right)
        auto cand = BoolMutexBatchOperator(&left_vals, &right_vals, &cmp_eff,
                                           bitlen, /*tid*/0, msgTagBase,
                                           SecureOperator::NO_CLIENT_COMPUTE)
                        .execute()->_zis;

        // 5) new_right = gate ? cand : right
        auto new_right = BoolMutexBatchOperator(&cand, &right_vals, &gate,
                                                bitlen, /*tid*/0, msgTagBase,
                                                SecureOperator::NO_CLIENT_COMPUTE)
                             .execute()->_zis;

        for (int i = 0; i < m; ++i) vs[i + delta] = new_right[i];

        // 6) 传播边界：b[i+delta] = b[i] OR b[i+delta] = ~(~b[i] & ~b[i+delta])
        std::vector<int64_t> not_l(m), not_r(m);
        for (int i = 0; i < m; ++i) {
            not_l[i] = bs_bool[i] ^ NOT_mask;
            not_r[i] = bs_bool[i + delta] ^ NOT_mask;
        }
        auto and_out = BoolAndBatchOperator(&not_l, &not_r, 1, 0, msgTagBase,
                                            SecureOperator::NO_CLIENT_COMPUTE)
                           .execute()->_zis;
        for (int i = 0; i < m; ++i) bs_bool[i + delta] = and_out[i] ^ NOT_mask;
    }

    // 组尾：tail[i] = head[i+1]；最后一行 = 1_share
    std::vector<int64_t> group_tails(n);
    for (size_t i = 0; i + 1 < n; ++i) group_tails[i] = heads[i + 1];
    group_tails[n - 1] = Comm::rank();

    // 插入最小值列（在 VALID 之前）
    const std::string outName = alias.empty() ? ("min_" + fieldName) : alias;

    _fieldNames.reserve(_fieldNames.size() + 1);
    _fieldWidths.reserve(_fieldWidths.size() + 1);
    _dataCols.reserve(_dataCols.size() + 1);

    const int insertPos = colNum() - 2;
    _fieldNames.insert(_fieldNames.begin() + insertPos, outName);
    _fieldWidths.insert(_fieldWidths.begin() + insertPos, bitlen);
    _dataCols.insert(_dataCols.begin() + insertPos, vs);

    // 更新有效位
    if (NO_COMPACTION) {
        // 不压缩：只在“原本有效 ∧ 组尾”的行上输出聚合结果
        auto new_valid = BoolAndBatchOperator(&_dataCols[validIdx], &group_tails, 1, 0, msgTagBase,
                                              SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        _dataCols[validIdx] = std::move(new_valid);
        // 不物理删除无效行
    } else {
        // 压缩：只保留组尾行
        _dataCols[validIdx] = std::move(group_tails);
        clearInvalidEntries(msgTagBase);
    }
}

// —— 多批：一轮一波并行（每批内：Less → Mutex(选较小) → Mutex(门控) → AND(边界)）
void View::minMultiBatches(std::vector<int64_t> &heads,
                           const std::string &fieldName,
                           std::string alias,
                           int msgTagBase) {
    const size_t n = rowNum();
    if (n == 0) return;

    const int fieldIdx = colIndex(fieldName);
    if (fieldIdx < 0) {
        Log::e("Field '{}' not found for min operation", fieldName);
        return;
    }
    const int bitlen = _fieldWidths[fieldIdx];

    auto &src = _dataCols[fieldIdx];
    if (src.size() != n || heads.size() != n) {
        Log::e("Size mismatch: fieldData={}, heads={}, n={}", src.size(), heads.size(), n);
        return;
    }

    const int batchSize = Conf::BATCH_SIZE;
    if (n <= static_cast<size_t>(batchSize)) {
        minSingleBatch(heads, fieldName, alias, msgTagBase);
        return;
    }

    std::vector<int64_t> vs = src; // 值工作副本
    std::vector<int64_t> bs_bool = heads; // 组头(XOR)
    const int64_t NOT_mask = Comm::rank();

    // tag stride 规划
    const int lessStride = BoolLessBatchOperator::tagStride();
    const int mutexStride = BoolMutexBatchOperator::tagStride();
    const int andStride = BoolAndBatchOperator::tagStride();
    const int taskStride = lessStride + mutexStride + mutexStride + andStride;

    int tagCursorBase = msgTagBase;

    for (int delta = 1; delta < static_cast<int>(n); delta <<= 1) {
        const int totalPairs = static_cast<int>(n) - delta;
        if (totalPairs <= 0) break;

        const int numBatches = (totalPairs + batchSize - 1) / batchSize;

        using Pair = std::pair<std::vector<int64_t>, std::vector<int64_t> >; // {new_right, new_b}
        std::vector<std::future<Pair> > futs(numBatches);

        for (int b = 0; b < numBatches; ++b) {
            const int start = b * batchSize;
            const int end = std::min(start + batchSize, totalPairs);
            const int len = end - start;

            const int baseTag = tagCursorBase + b * taskStride;
            const int lessTag = baseTag;
            const int mtx1Tag = baseTag + lessStride;
            const int mtx2Tag = mtx1Tag + mutexStride;
            const int andTag = mtx2Tag + mutexStride;

            futs[b] = ThreadPoolSupport::submit([=, &vs, &bs_bool]() -> Pair {
                if (len <= 0) return Pair{{}, {}};

                std::vector<int64_t> left_vals(len), right_vals(len);
                std::vector<int64_t> not_bL(len), not_bR(len);
                for (int i = 0; i < len; ++i) {
                    const int idxL = start + i;
                    const int idxR = idxL + delta;
                    left_vals[i] = vs[idxL];
                    right_vals[i] = vs[idxR];
                    not_bL[i] = bs_bool[idxL] ^ NOT_mask; // ~b[i]
                    not_bR[i] = bs_bool[idxR] ^ NOT_mask; // ~b[i+delta]
                }

                // 1) cmp = (left < right)
                auto cmp = BoolLessBatchOperator(&left_vals, &right_vals, bitlen,
                                                 0, lessTag,
                                                 SecureOperator::NO_CLIENT_COMPUTE)
                        .execute()->_zis;

                // 2) cand = cmp ? left : right   —— 取较小值
                auto cand = BoolMutexBatchOperator(&left_vals, &right_vals, &cmp,
                                                   bitlen, 0, mtx1Tag,
                                                   SecureOperator::NO_CLIENT_COMPUTE)
                        .execute()->_zis;

                // 3) new_right = (~bR) ? cand : right
                auto new_right = BoolMutexBatchOperator(&cand, &right_vals, &not_bR,
                                                        bitlen, 0, mtx2Tag,
                                                        SecureOperator::NO_CLIENT_COMPUTE)
                        .execute()->_zis;

                // 4) new_b = bL OR bR = ~(~bL & ~bR)
                auto and_out = BoolAndBatchOperator(&not_bL, &not_bR, 1, 0, andTag,
                                                    SecureOperator::NO_CLIENT_COMPUTE)
                        .execute()->_zis;
                std::vector<int64_t> new_b(len);
                for (int i = 0; i < len; ++i) new_b[i] = and_out[i] ^ NOT_mask;

                return Pair{std::move(new_right), std::move(new_b)};
            });
        }

        tagCursorBase += numBatches * taskStride;

        // 写回右端
        for (int b = 0; b < numBatches; ++b) {
            const int start = b * batchSize;
            const int end = std::min(start + batchSize, totalPairs);
            const int len = end - start;
            if (len <= 0) continue;

            auto pr = futs[b].get();
            auto &new_right = pr.first;
            auto &new_b = pr.second;
            for (int i = 0; i < len; ++i) {
                const int idxR = start + i + delta;
                vs[idxR] = new_right[i];
                bs_bool[idxR] = new_b[i];
            }
        }
    }

    // 组尾：tail[i] = head[i+1]；最后一行 = 1_share
    std::vector<int64_t> group_tails(n);
    for (size_t i = 0; i + 1 < n; ++i) group_tails[i] = heads[i + 1];
    group_tails[n - 1] = Comm::rank();

    // 插入结果列（在 VALID 之前）
    const std::string outName = alias.empty() ? ("min_" + fieldName) : alias;

    _fieldNames.reserve(_fieldNames.size() + 1);
    _fieldWidths.reserve(_fieldWidths.size() + 1);
    _dataCols.reserve(_dataCols.size() + 1);

    const int insertPos = colNum() - 2;
    _fieldNames.insert(_fieldNames.begin() + insertPos, outName);
    _fieldWidths.insert(_fieldWidths.begin() + insertPos, bitlen);
    _dataCols.insert(_dataCols.begin() + insertPos, std::move(vs));

    _dataCols[colNum() + VALID_COL_OFFSET] = std::move(group_tails);
    clearInvalidEntries(msgTagBase);
}

int View::sortTagStride() {
    return ((rowNum() / 2 + Conf::BATCH_SIZE - 1) / Conf::BATCH_SIZE) * (colNum() - 1) *
           BoolMutexBatchOperator::tagStride();
}

void View::filterSingleBatch(std::vector<std::string> &fieldNames,
                             std::vector<ComparatorType> &comparatorTypes,
                             std::vector<int64_t> &constShares, int msgTagBase) {
    size_t n = fieldNames.size();
    size_t data_size = _dataCols[0].size();
    std::vector<std::vector<int64_t> > collected(n);

    // Process each condition sequentially (single batch approach)
    for (int i = 0; i < n; i++) {
        int colIndex = 0;
        for (int j = 0; j < _fieldNames.size(); j++) {
            if (_fieldNames[j] == fieldNames[i]) {
                colIndex = j;
                break;
            }
        }
        auto ct = comparatorTypes[i];

        std::vector<int64_t> batch_const(data_size, constShares[i]);
        std::vector<int64_t> &batch_data = _dataCols[colIndex];

        std::vector<int64_t> batch_result;

        switch (ct) {
            case GREATER:
            case LESS_EQ: {
                batch_result = BoolLessBatchOperator(&batch_const, &batch_data,
                                                     _fieldWidths[colIndex], 0, msgTagBase,
                                                     SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
                if (ct == LESS_EQ) {
                    for (auto &v: batch_result) {
                        v ^= Comm::rank();
                    }
                }
                break;
            }
            case LESS:
            case GREATER_EQ: {
                batch_result = BoolLessBatchOperator(&batch_data, &batch_const,
                                                     _fieldWidths[colIndex], 0, msgTagBase,
                                                     SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
                if (ct == GREATER_EQ) {
                    for (auto &v: batch_result) {
                        v ^= Comm::rank();
                    }
                }
                break;
            }
            case EQUALS:
            case NOT_EQUALS: {
                batch_result = BoolEqualBatchOperator(&batch_const, &batch_data, _fieldWidths[colIndex],
                                                      0, msgTagBase,
                                                      SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
                if (ct == NOT_EQUALS) {
                    for (auto &v: batch_result) {
                        v ^= Comm::rank();
                    }
                }
                break;
            }
        }

        collected[i] = std::move(batch_result);
    }

    int validColIndex = colNum() + VALID_COL_OFFSET;
    if (n == 1) {
        _dataCols[validColIndex] = collected[0];
        return;
    }

    // Combine multiple conditions with AND operation (single batch approach)
    std::vector<int64_t> result = std::move(collected[0]);
    for (int i = 1; i < n; i++) {
        result = BoolAndBatchOperator(&result, &collected[i], 1, 0, msgTagBase,
                                      SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    }

    if (DbConf::DISABLE_COMPACTION) {
        _dataCols[validColIndex] = BoolAndBatchOperator(&result, &_dataCols[validColIndex], 1, 0, msgTagBase,
                                                        SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    } else {
        _dataCols[validColIndex] = std::move(result);
        clearInvalidEntries(msgTagBase);
    }
}

void View::filterMultiBatches(std::vector<std::string> &fieldNames, std::vector<ComparatorType> &comparatorTypes,
                              std::vector<int64_t> &constShares, int msgTagBase) {
    if (rowNum() <= Conf::BATCH_SIZE) {
        filterSingleBatch(fieldNames, comparatorTypes, constShares, msgTagBase);
        return;
    }
    size_t n = fieldNames.size();
    std::vector<std::vector<int64_t> > collected(n);

    std::vector<std::future<std::vector<int64_t> > > futures(n);
    size_t data_size = _dataCols[0].size();
    int batchSize = Conf::BATCH_SIZE;
    int batchNum = (data_size + batchSize - 1) / batchSize;
    int tagOffset = std::max(BoolLessBatchOperator::tagStride(), BoolEqualBatchOperator::tagStride());

    for (int i = 0; i < n; i++) {
        futures[i] = ThreadPoolSupport::submit([&, i] {
            int colIndex = 0;
            for (int j = 0; j < _fieldNames.size(); j++) {
                if (_fieldNames[j] == fieldNames[i]) {
                    colIndex = j;
                    break;
                }
            }
            auto ct = comparatorTypes[i];

            std::vector<std::future<std::vector<int64_t> > > batch_futures(batchNum);
            int startTag = tagOffset * batchNum * i;

            for (int b = 0; b < batchNum; ++b) {
                int start = b * batchSize;
                int end = std::min(start + batchSize, static_cast<int>(data_size));
                int currentBatchSize = end - start;

                batch_futures[b] = ThreadPoolSupport::submit(
                    [&, start, end, ct, colIndex, startTag, b, currentBatchSize]() {
                        std::vector<int64_t> batch_const(currentBatchSize, constShares[i]);
                        auto batch_data_begin = _dataCols[colIndex].begin() + start;
                        auto batch_data_end = _dataCols[colIndex].begin() + end;
                        std::vector<int64_t> batch_data(batch_data_begin, batch_data_end);

                        int batch_start_tag = startTag + b * tagOffset;
                        std::vector<int64_t> batch_result;

                        switch (ct) {
                            case GREATER:
                            case LESS_EQ: {
                                batch_result = BoolLessBatchOperator(&batch_const, &batch_data,
                                                                     _fieldWidths[colIndex], 0, batch_start_tag,
                                                                     SecureOperator::NO_CLIENT_COMPUTE).execute()->
                                        _zis;
                                if (ct == LESS_EQ) {
                                    for (auto &v: batch_result) {
                                        v ^= Comm::rank();
                                    }
                                }
                                break;
                            }
                            case LESS:
                            case GREATER_EQ: {
                                batch_result = BoolLessBatchOperator(&batch_data, &batch_const,
                                                                     _fieldWidths[colIndex], 0, batch_start_tag,
                                                                     SecureOperator::NO_CLIENT_COMPUTE).execute()->
                                        _zis;
                                if (ct == GREATER_EQ) {
                                    for (auto &v: batch_result) {
                                        v ^= Comm::rank();
                                    }
                                }
                                break;
                            }
                            case EQUALS:
                            case NOT_EQUALS: {
                                batch_result = BoolEqualBatchOperator(&batch_const, &batch_data, _fieldWidths[colIndex],
                                                                      0, batch_start_tag,
                                                                      SecureOperator::NO_CLIENT_COMPUTE).execute()->
                                        _zis;
                                if (ct == NOT_EQUALS) {
                                    for (auto &v: batch_result) {
                                        v ^= Comm::rank();
                                    }
                                }
                                break;
                            }
                        }

                        return batch_result;
                    });
            }

            std::vector<int64_t> currentCondition;
            currentCondition.reserve(data_size);
            for (auto &f: batch_futures) {
                auto batch_res = f.get();
                currentCondition.insert(currentCondition.end(), batch_res.begin(), batch_res.end());
            }

            return currentCondition;
        });
    }

    for (int i = 0; i < n; i++) {
        collected[i] = futures[i].get();
    }

    int validColIndex = colNum() + VALID_COL_OFFSET;
    if (n == 1) {
        _dataCols[validColIndex] = collected[0];
        return;
    }

    std::vector<std::vector<int64_t> > andCols = std::move(collected);
    int level = 0;
    while (andCols.size() > 1) {
        int m = static_cast<int>(andCols.size());
        int pairs = m / 2;
        int msgStride = BoolAndBatchOperator::tagStride();

        std::vector<std::future<std::vector<int64_t> > > futures1;
        futures1.reserve(pairs);

        for (int p = 0; p < pairs; ++p) {
            futures1.emplace_back(
                ThreadPoolSupport::submit(
                    [&, p, level]() {
                        auto &L = andCols[2 * p];
                        auto &R = andCols[2 * p + 1];
                        return BoolAndBatchOperator(
                            &L, &R, 1,
                            0,
                            msgTagBase + level * msgStride + p, SecureOperator::NO_CLIENT_COMPUTE
                        ).execute()->_zis;
                    }
                )
            );
        }

        std::vector<std::vector<int64_t> > nextCols;
        nextCols.reserve((m + 1) / 2);

        for (int p = 0; p < pairs; ++p) {
            nextCols.push_back(futures1[p].get());
        }
        if (m % 2) {
            nextCols.push_back(std::move(andCols.back()));
        }

        andCols.swap(nextCols);
        ++level;


        if (!andCols.empty()) {
            _dataCols[validColIndex] = std::move(andCols.front());
        }
    }
    clearInvalidEntries(msgTagBase);
}

void View::filterAndConditions(std::vector<std::string> &fieldNames, std::vector<ComparatorType> &comparatorTypes,
                               std::vector<int64_t> &constShares, int msgTagBase) {
    if (Conf::BATCH_SIZE <= 0 || Conf::DISABLE_MULTI_THREAD) {
        filterSingleBatch(fieldNames, comparatorTypes, constShares, msgTagBase);
    } else {
        filterMultiBatches(fieldNames, comparatorTypes, constShares, msgTagBase);
    }
}

void View::clearInvalidEntries(int msgTagBase) {
    // sort view by valid column
    sort(VALID_COL_NAME, false, msgTagBase);
    int64_t sumShare = 0, sumShare1;
    if (Conf::DISABLE_MULTI_THREAD || Conf::BATCH_SIZE <= 0) {
        // compute valid num
        auto ta = BoolToArithBatchOperator(&_dataCols[_dataCols.size() + VALID_COL_OFFSET], 64, 0, msgTagBase,
                                           SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        sumShare = std::accumulate(ta.begin(), ta.end(), 0ll);
    } else {
        size_t batchSize = Conf::BATCH_SIZE;
        size_t n = rowNum();
        size_t batchNum = (n + batchSize - 1) / batchSize;
        std::vector<std::future<std::vector<int64_t> > > futures(batchNum);

        for (int i = 0; i < batchNum; ++i) {
            futures[i] = ThreadPoolSupport::submit([this, i, batchSize, n, msgTagBase] {
                auto &validCol = _dataCols[_dataCols.size() + VALID_COL_OFFSET];
                std::vector part(validCol.begin() + batchSize * i,
                                 validCol.begin() + std::min(batchSize * (i + 1), n));
                return BoolToArithBatchOperator(&part, 64, 0, msgTagBase + BoolToArithBatchOperator::tagStride() * i,
                                                SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
            });
        }

        for (auto &f: futures) {
            auto temp = f.get();
            for (auto e: temp) {
                sumShare += e;
            }
        }
    }

    auto r0 = Comm::serverSendAsync(sumShare, 32, msgTagBase);
    auto r1 = Comm::serverReceiveAsync(sumShare1, 32, msgTagBase);
    Comm::wait(r0);
    Comm::wait(r1);

    int64_t validNum = sumShare + sumShare1;

    for (auto &v: _dataCols) {
        v.resize(validNum);
    }

    // Not Oblivious Version
    // std::vector<std::vector<int64_t>> newCols(colNum());
    // for (int i = 0; i < rowNum(); i++) {
    //     bool valid0 = _dataCols[colNum() + VALID_COL_OFFSET][i];
    //     int64_t valid1;
    //     auto r0 = Comm::sendAsync(valid0, 1, 1 - Comm::rank(), 0);
    //     auto r1 = Comm::receiveAsync(valid1, 1, 1 - Comm::rank(), 0);
    //     Comm::wait(r0);
    //     Comm::wait(r1);
    //     bool valid = valid0 ^ valid1;
    //     if (valid) {
    //         for (int j = 0; j < colNum(); j++) {
    //             newCols[j].push_back(_dataCols[j][i]);
    //         }
    //     }
    // }
    // _dataCols = std::move(newCols);
}

int View::clearInvalidEntriesTagStride() {
    return std::max(sortTagStride(),
                    static_cast<int>((rowNum() + Conf::BATCH_SIZE - 1) / Conf::BATCH_SIZE) *
                    BoolToArithBatchOperator::tagStride());
}

void View::addRedundantCols() {
    _fieldNames.emplace_back(View::VALID_COL_NAME);
    _fieldWidths.emplace_back(1);
    _dataCols.emplace_back(_dataCols[0].size(), Comm::rank());

    _fieldNames.emplace_back(View::PADDING_COL_NAME);
    _fieldWidths.emplace_back(1);
    _dataCols.emplace_back(_dataCols[0].size());
}

void View::bitonicSortSingleBatch(const std::string &orderField, bool ascendingOrder, int msgTagBase) {
    auto n = _dataCols[0].size();
    int ofi = colIndex(orderField);
    auto &orderCol = _dataCols[ofi];
    auto &paddings = _dataCols[colNum() + PADDING_COL_OFFSET];
    for (int k = 2; k <= n; k *= 2) {
        for (int j = k / 2; j > 0; j /= 2) {
            std::vector<int64_t> xs;
            std::vector<int64_t> ys;
            std::vector<int64_t> xIdx;
            std::vector<int64_t> yIdx;
            std::vector<bool> ascs;

            size_t halfN = n / 2;
            xs.reserve(halfN);
            ys.reserve(halfN);
            xIdx.reserve(halfN);
            yIdx.reserve(halfN);
            ascs.reserve(halfN);

            for (int i = 0; i < n; i++) {
                int l = i ^ j;
                if (l <= i) {
                    continue;
                }
                bool dir = (i & k) == 0;
                // last col is padding bool
                if (paddings[i] && paddings[l]) {
                    continue;
                }
                if ((paddings[i] && dir) || (paddings[l] && !dir)) {
                    for (auto &v: _dataCols) {
                        std::swap(v[i], v[l]);
                    }
                    continue;
                }
                if (paddings[i] || paddings[l]) {
                    continue;
                }
                xs.push_back(orderCol[i]);
                xIdx.push_back(i);
                ys.push_back(orderCol[l]);
                yIdx.push_back(l);
                ascs.push_back(dir ^ !ascendingOrder);
            }

            size_t comparingCount = xs.size();
            if (comparingCount == 0) {
                continue;
            }

            std::vector<int64_t> zs;
            zs = BoolLessBatchOperator(&xs, &ys, _fieldWidths[ofi], 0, msgTagBase,
                                       SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

            for (int i = 0; i < comparingCount; i++) {
                if (!ascs[i]) {
                    zs[i] = zs[i] ^ Comm::rank();
                }
            }

            size_t sz = comparingCount * (colNum() - 1);
            xs.reserve(sz);
            ys.reserve(sz);
            // xs and ys has already stored one order column
            for (int i = 0; i < colNum() - 1; i++) {
                if (i == ofi) {
                    continue;
                }
                for (int m = 0; m < comparingCount; m++) {
                    xs.push_back(_dataCols[i][xIdx[m]]);
                    ys.push_back(_dataCols[i][yIdx[m]]);
                }
            }

            zs = BoolMutexBatchOperator(&xs, &ys, &zs, _maxWidth, 0, msgTagBase).
                    execute()->_zis;

            for (int i = 0; i < comparingCount; i++) {
                orderCol[xIdx[i]] = Math::ring(zs[i], _fieldWidths[ofi]);
                orderCol[yIdx[i]] = Math::ring(zs[(colNum() - 1) * comparingCount + i], _fieldWidths[ofi]);
            }

            // skip first for order column
            int pos = 1;
            for (int i = 0; i < colNum() - 1; i++) {
                if (i == ofi) {
                    continue;
                }
                auto &co = _dataCols[i];
                for (int m = 0; m < comparingCount; m++) {
                    co[xIdx[m]] = Math::ring(zs[pos * comparingCount + m], _fieldWidths[i]);
                    co[yIdx[m]] = Math::ring(zs[(pos + colNum() - 1) * comparingCount + m], _fieldWidths[i]);
                }
                pos++;
            }
        }
    }
}

void View::bitonicSortMultiBatches(const std::string &orderField, bool ascendingOrder, int msgTagBase) {
    const int batchSize = Conf::BATCH_SIZE;
    const int n = static_cast<int>(rowNum());
    int ofi = colIndex(orderField);
    auto &orderCol = _dataCols[ofi];
    auto &paddings = _dataCols[colNum() + PADDING_COL_OFFSET];
    for (int k = 2; k <= n; k <<= 1) {
        for (int j = k >> 1; j > 0; j >>= 1) {
            size_t halfN = n / 2;
            std::vector<int> xIdx, yIdx;
            std::vector<bool> ascs;
            xIdx.reserve(halfN);
            yIdx.reserve(halfN);
            ascs.reserve(halfN);
            for (int i = 0; i < n; ++i) {
                int l = i ^ j;
                if (l <= i) {
                    continue;
                }
                bool dir = (i & k) == 0;
                if (paddings[i] && paddings[l]) {
                    continue;
                }
                if ((paddings[i] && dir) || (paddings[l] && !dir)) {
                    for (auto &v: _dataCols) {
                        std::swap(v[i], v[l]);
                    }
                    continue;
                }
                if (paddings[i] || paddings[l]) {
                    continue;
                }
                xIdx.push_back(i);
                yIdx.push_back(l);
                ascs.push_back(dir ^ !ascendingOrder);
            }

            int comparingCount = static_cast<int>(xIdx.size());
            if (comparingCount == 0) {
                continue;
            }

            const int numBatches = (comparingCount + batchSize - 1) / batchSize;
            const int tagStride = static_cast<int>(BoolMutexBatchOperator::tagStride() * (colNum() - 1));
            std::vector<std::future<void> > futures;
            futures.reserve(numBatches);
            for (int b = 0; b < numBatches; ++b) {
                futures.emplace_back(ThreadPoolSupport::submit([&, b]() {
                    const int start = b * batchSize;
                    const int end = std::min(start + batchSize, comparingCount);
                    const int cnt = end - start;
                    std::vector<int64_t> xs, ys;
                    xs.reserve(cnt);
                    ys.reserve(cnt);
                    std::vector<bool> ascs2;
                    ascs2.reserve(cnt);
                    for (int t = start; t < end; ++t) {
                        xs.push_back(orderCol[xIdx[t]]);
                        ys.push_back(orderCol[yIdx[t]]);
                        ascs2.push_back(ascs[t]);
                    }
                    auto zs = BoolLessBatchOperator(&xs, &ys, _fieldWidths[ofi], 0,
                                                    msgTagBase + tagStride * b,
                                                    SecureOperator::NO_CLIENT_COMPUTE).execute()->
                            _zis;
                    for (int t = 0; t < cnt; ++t) {
                        if (!ascs2[t]) {
                            zs[t] ^= Comm::rank();
                        }
                    }

                    // parallel process each column (except padding column)
                    std::vector<std::future<void> > futures2;
                    futures2.reserve(colNum() - 1);
                    for (int c = 0; c < colNum() - 1; ++c) {
                        futures2.push_back(ThreadPoolSupport::submit([&, b, c] {
                            auto &col = _dataCols[c];
                            std::vector<int64_t> subXs, subYs;
                            if (_fieldNames[c] == orderField) {
                                subXs = std::move(xs);
                                subYs = std::move(ys);
                            } else {
                                subXs.reserve(cnt);
                                subYs.reserve(cnt);
                                for (int t = start; t < end; ++t) {
                                    subXs.push_back(col[xIdx[t]]);
                                    subYs.push_back(col[yIdx[t]]);
                                }
                            }

                            auto copy = zs;
                            auto zs2 = BoolMutexBatchOperator(&subXs, &subYs, &copy, _fieldWidths[c], 0,
                                                              msgTagBase + tagStride * b +
                                                              BoolMutexBatchOperator::tagStride() * c).execute()->
                                    _zis;

                            int64_t time = System::currentTimeMillis();
                            for (int t = 0; t < cnt; ++t) {
                                col[xIdx[start + t]] = zs2[t];
                                col[yIdx[start + t]] = zs2[cnt + t];
                            }
                        }));
                    }
                    for (auto &f: futures2) {
                        f.wait();
                    }
                }));
            }
            for (auto &f: futures) {
                f.wait();
            }
        }
    }
}


// —— 单批：同时计算最小值和最大值（Hillis–Steele 扫描 + 组边界传播）
void View::minAndMaxSingleBatch(std::vector<int64_t> &heads,
                                const std::string &fieldName,
                                std::string minAlias,
                                std::string maxAlias,
                                int msgTagBase) {
    const size_t n = rowNum();
    if (n == 0) return;

    const int fieldIdx = colIndex(fieldName);
    if (fieldIdx < 0) {
        Log::e("Field '{}' not found for minAndMax operation", fieldName);
        return;
    }
    const int bitlen = _fieldWidths[fieldIdx];

    std::vector<int64_t> &src = _dataCols[fieldIdx];
    if (src.size() != n || heads.size() != n) {
        Log::e("Size mismatch: fieldData={}, heads={}, n={}", src.size(), heads.size(), n);
        return;
    }

    // 工作副本
    std::vector<int64_t> min_vs = src;
    std::vector<int64_t> max_vs = src;
    std::vector<int64_t> bs_bool = heads;

    const int64_t NOT_mask = Comm::rank();
    const bool NO_COMPACTION = DbConf::DISABLE_COMPACTION;

    // 仅在“不压缩”模式下需要参与聚合的有效位
    std::vector<int64_t> valid_col;
    if (NO_COMPACTION) {
        valid_col = _dataCols[colNum() + VALID_COL_OFFSET]; // 布尔分享
    }

    // === 分段扫描（并行前缀） ===
    // tag 规划（避免冲突）
    int tag_off_less = BoolLessBatchOperator::tagStride();
    int tag_off_and  = BoolAndBatchOperator::tagStride();
    int tag_off_mux  = BoolMutexBatchOperator::tagStride();

    for (int delta = 1, round = 0; delta < static_cast<int>(n); delta <<= 1, ++round) {
        const int m = static_cast<int>(n) - delta;

        // 快照左右值
        std::vector<int64_t> min_L(m), min_R(m), max_L(m), max_R(m);
        for (int i = 0; i < m; ++i) {
            min_L[i] = min_vs[i];
            min_R[i] = min_vs[i + delta];
            max_L[i] = max_vs[i];
            max_R[i] = max_vs[i + delta];
        }

        // 左<右 比较
        auto less_min = BoolLessBatchOperator(&min_L, &min_R, bitlen, 0,
                          msgTagBase + round * (tag_off_less + tag_off_and + 2 * tag_off_mux),
                          SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        auto less_max = BoolLessBatchOperator(&max_L, &max_R, bitlen, 0,
                          msgTagBase + round * (tag_off_less + tag_off_and + 2 * tag_off_mux) + tag_off_less,
                          SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        // 构造选择掩码（不压缩时使用有效位；压缩时退化为原 cmp）
        std::vector<int64_t> sel_min = less_min; // 默认：压缩模式
        std::vector<int64_t> sel_max = less_max;

        if (NO_COMPACTION) {
            // vl, vr：左右位置有效位
            std::vector<int64_t> vl(m), vr(m), not_vl(m), not_vr(m);
            for (int i = 0; i < m; ++i) {
                vl[i] = valid_col[i];
                vr[i] = valid_col[i + delta];
                not_vl[i] = vl[i] ^ NOT_mask;
                not_vr[i] = vr[i] ^ NOT_mask;
            }

            // sel_min = vl ∧ ( ¬vr ∨ (left<right) )
            // 先做 (¬vr ∨ less_min) = ¬( vr ∧ ¬less_min )
            std::vector<int64_t> not_less_min(m);
            for (int i = 0; i < m; ++i) not_less_min[i] = less_min[i] ^ NOT_mask;
            auto vr_and_not_less = BoolAndBatchOperator(&vr, &not_less_min, 1, 0,
                                       msgTagBase + round * (tag_off_less + tag_off_and + 2 * tag_off_mux) + 2*tag_off_less,
                                       SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
            std::vector<int64_t> or_notvr_or_less(m);
            for (int i = 0; i < m; ++i) or_notvr_or_less[i] = vr_and_not_less[i] ^ NOT_mask;
            sel_min = BoolAndBatchOperator(&vl, &or_notvr_or_less, 1, 0,
                           msgTagBase + round * (tag_off_less + tag_off_and + 2 * tag_off_mux) + 2*tag_off_less + tag_off_and,
                           SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

            // sel_max = (¬vl) ∨ (vl ∧ vr ∧ less_max)
            auto vl_and_vr = BoolAndBatchOperator(&vl, &vr, 1, 0,
                              msgTagBase + round * (tag_off_less + tag_off_and + 2 * tag_off_mux) + 2*tag_off_less + 2*tag_off_and,
                              SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
            auto and_all   = BoolAndBatchOperator(&vl_and_vr, &less_max, 1, 0,
                              msgTagBase + round * (tag_off_less + tag_off_and + 2 * tag_off_mux) + 2*tag_off_less + 3*tag_off_and,
                              SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
            // OR via DeMorgan: (¬vl) ∨ and_all = ¬( vl ∧ ¬and_all )
            std::vector<int64_t> not_and_all(m);
            for (int i = 0; i < m; ++i) not_and_all[i] = and_all[i] ^ NOT_mask;
            auto vl_and_not = BoolAndBatchOperator(&vl, &not_and_all, 1, 0,
                               msgTagBase + round * (tag_off_less + tag_off_and + 2 * tag_off_mux) + 2*tag_off_less + 4*tag_off_and,
                               SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
            sel_max.resize(m);
            for (int i = 0; i < m; ++i) sel_max[i] = vl_and_not[i] ^ NOT_mask;
        }

        // 候选：min / max
        auto min_cand = BoolMutexBatchOperator(&min_L, &min_R, &sel_min, bitlen, 0,
                          msgTagBase + round * (tag_off_less + tag_off_and + 2 * tag_off_mux) + 2*tag_off_less + 5*tag_off_and,
                          SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        auto max_cand = BoolMutexBatchOperator(&max_R, &max_L, &sel_max, bitlen, 0,
                          msgTagBase + round * (tag_off_less + tag_off_and + 2 * tag_off_mux) + 2*tag_off_less + 5*tag_off_and + tag_off_mux,
                          SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        // gate = ¬b[i+δ]（仍按原分段逻辑）
        std::vector<int64_t> gate(m);
        for (int i = 0; i < m; ++i) gate[i] = (bs_bool[i + delta] ^ NOT_mask);

        // new_right = gate ? cand : right
        auto min_new_right = BoolMutexBatchOperator(&min_cand, &min_R, &gate, bitlen, 0,
                              msgTagBase + round * (tag_off_less + tag_off_and + 2 * tag_off_mux) + 2*tag_off_less + 5*tag_off_and + 2*tag_off_mux,
                              SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        auto max_new_right = BoolMutexBatchOperator(&max_cand, &max_R, &gate, bitlen, 0,
                              msgTagBase + round * (tag_off_less + tag_off_and + 2 * tag_off_mux) + 2*tag_off_less + 5*tag_off_and + 3*tag_off_mux,
                              SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        for (int i = 0; i < m; ++i) {
            min_vs[i + delta] = min_new_right[i];
            max_vs[i + delta] = max_new_right[i];
        }

        // 传播边界：b[i+δ] = b[i] ∨ b[i+δ] = ¬(¬b[i] ∧ ¬b[i+δ])
        std::vector<int64_t> not_l(m), not_r(m);
        for (int i = 0; i < m; ++i) {
            not_l[i] = bs_bool[i] ^ NOT_mask;
            not_r[i] = bs_bool[i + delta] ^ NOT_mask;
        }
        auto and_out = BoolAndBatchOperator(&not_l, &not_r, 1, 0,
                         msgTagBase + round * (tag_off_less + tag_off_and + 2 * tag_off_mux) + 2*tag_off_less + 5*tag_off_and + 4*tag_off_mux,
                         SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        for (int i = 0; i < m; ++i) bs_bool[i + delta] = and_out[i] ^ NOT_mask;
    }

    // 组尾：tail[i] = head[i+1]；最后一行 = 1_share
    std::vector<int64_t> group_tails(n);
    for (size_t i = 0; i + 1 < n; ++i) group_tails[i] = heads[i + 1];
    group_tails[n - 1] = Comm::rank();

    // 输出列名与插入位置（在 VALID 之前）
    const std::string minOutName = minAlias.empty() ? ("min_" + fieldName) : minAlias;
    const std::string maxOutName = maxAlias.empty() ? ("max_" + fieldName) : maxAlias;

    const int insertPos = colNum() - 2;
    _fieldNames.insert(_fieldNames.begin() + insertPos, minOutName);
    _fieldWidths.insert(_fieldWidths.begin() + insertPos, bitlen);
    _dataCols.insert(_dataCols.begin() + insertPos, min_vs);

    _fieldNames.insert(_fieldNames.begin() + insertPos + 1, maxOutName);
    _fieldWidths.insert(_fieldWidths.begin() + insertPos + 1, bitlen);
    _dataCols.insert(_dataCols.begin() + insertPos + 1, max_vs);

    int validIdx = colNum() + VALID_COL_OFFSET;

    if (NO_COMPACTION) {
        // 不压缩：valid := valid ∧ group_tails
        auto new_valid = BoolAndBatchOperator(&_dataCols[validIdx], &group_tails, 1, 0,
                          msgTagBase, SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        _dataCols[validIdx] = std::move(new_valid);
        // 不调用 clearInvalidEntries()
    } else {
        // 压缩：只保留每组尾行
        _dataCols[validIdx] = std::move(group_tails);
        clearInvalidEntries(msgTagBase);
    }
}

// —— 多批：同时计算最小值和最大值（一轮一波并行）
void View::minAndMaxMultiBatches(std::vector<int64_t> &heads,
                                 const std::string &fieldName,
                                 std::string minAlias,
                                 std::string maxAlias,
                                 int msgTagBase) {
    const size_t n = rowNum();
    if (n == 0) return;

    const int fieldIdx = colIndex(fieldName);
    if (fieldIdx < 0) {
        Log::e("Field '{}' not found for minAndMax operation", fieldName);
        return;
    }
    const int bitlen = _fieldWidths[fieldIdx];

    auto &src = _dataCols[fieldIdx];
    if (src.size() != n || heads.size() != n) {
        Log::e("Size mismatch: fieldData={}, heads={}, n={}", src.size(), heads.size(), n);
        return;
    }

    const int batchSize = Conf::BATCH_SIZE;
    if (n <= static_cast<size_t>(batchSize)) {
        minAndMaxSingleBatch(heads, fieldName, minAlias, maxAlias, msgTagBase);
        return;
    }

    std::vector<int64_t> min_vs = src; // 最小值工作副本
    std::vector<int64_t> max_vs = src; // 最大值工作副本
    std::vector<int64_t> bs_bool = heads; // 组头(XOR)
    const int64_t NOT_mask = Comm::rank();

    // tag stride 规划（需要更多的tag因为有两套操作）
    const int lessStride = BoolLessBatchOperator::tagStride();
    const int mutexStride = BoolMutexBatchOperator::tagStride();
    const int andStride = BoolAndBatchOperator::tagStride();
    // 每个批任务用到：Less(2) + Mutex(4) + AND(1)
    const int taskStride = 2 * lessStride + 4 * mutexStride + andStride;

    int tagCursorBase = msgTagBase;

    for (int delta = 1; delta < static_cast<int>(n); delta <<= 1) {
        const int totalPairs = static_cast<int>(n) - delta;
        if (totalPairs <= 0) break;

        const int numBatches = (totalPairs + batchSize - 1) / batchSize;

        using Triple = std::tuple<std::vector<int64_t>, std::vector<int64_t>, std::vector<int64_t> >;
        // {min_new_right, max_new_right, new_b}
        std::vector<std::future<Triple> > futs(numBatches);

        for (int b = 0; b < numBatches; ++b) {
            const int start = b * batchSize;
            const int end = std::min(start + batchSize, totalPairs);
            const int len = end - start;

            const int baseTag = tagCursorBase + b * taskStride;
            const int minLessTag = baseTag;
            const int maxLessTag = baseTag + lessStride;
            const int minMtx1Tag = maxLessTag + lessStride;
            const int maxMtx1Tag = minMtx1Tag + mutexStride;
            const int minMtx2Tag = maxMtx1Tag + mutexStride;
            const int maxMtx2Tag = minMtx2Tag + mutexStride;
            const int andTag = maxMtx2Tag + mutexStride;

            futs[b] = ThreadPoolSupport::submit([=, &min_vs, &max_vs, &bs_bool]() -> Triple {
                if (len <= 0) return Triple{{}, {}, {}};

                std::vector<int64_t> min_left_vals(len), min_right_vals(len);
                std::vector<int64_t> max_left_vals(len), max_right_vals(len);
                std::vector<int64_t> not_bL(len), not_bR(len);
                for (int i = 0; i < len; ++i) {
                    const int idxL = start + i;
                    const int idxR = idxL + delta;
                    min_left_vals[i] = min_vs[idxL];
                    min_right_vals[i] = min_vs[idxR];
                    max_left_vals[i] = max_vs[idxL];
                    max_right_vals[i] = max_vs[idxR];
                    not_bL[i] = bs_bool[idxL] ^ NOT_mask; // ~b[i]
                    not_bR[i] = bs_bool[idxR] ^ NOT_mask; // ~b[i+delta]
                }

                // 1) 比较操作
                auto min_cmp = BoolLessBatchOperator(&min_left_vals, &min_right_vals, bitlen,
                                                     0, minLessTag,
                                                     SecureOperator::NO_CLIENT_COMPUTE)
                        .execute()->_zis;
                auto max_cmp = BoolLessBatchOperator(&max_left_vals, &max_right_vals, bitlen,
                                                     0, maxLessTag,
                                                     SecureOperator::NO_CLIENT_COMPUTE)
                        .execute()->_zis;

                // 2) 选择操作：min取较小值，max取较大值
                auto min_cand = BoolMutexBatchOperator(&min_left_vals, &min_right_vals, &min_cmp,
                                                       bitlen, 0, minMtx1Tag,
                                                       SecureOperator::NO_CLIENT_COMPUTE)
                        .execute()->_zis;
                auto max_cand = BoolMutexBatchOperator(&max_right_vals, &max_left_vals, &max_cmp,
                                                       bitlen, 0, maxMtx1Tag,
                                                       SecureOperator::NO_CLIENT_COMPUTE)
                        .execute()->_zis;

                // 3) 门控操作
                auto min_new_right = BoolMutexBatchOperator(&min_cand, &min_right_vals, &not_bR,
                                                            bitlen, 0, minMtx2Tag,
                                                            SecureOperator::NO_CLIENT_COMPUTE)
                        .execute()->_zis;
                auto max_new_right = BoolMutexBatchOperator(&max_cand, &max_right_vals, &not_bR,
                                                            bitlen, 0, maxMtx2Tag,
                                                            SecureOperator::NO_CLIENT_COMPUTE)
                        .execute()->_zis;

                // 4) 边界传播：new_b = bL OR bR = ~(~bL & ~bR)
                auto and_out = BoolAndBatchOperator(&not_bL, &not_bR, 1, 0, andTag,
                                                    SecureOperator::NO_CLIENT_COMPUTE)
                        .execute()->_zis;
                std::vector<int64_t> new_b(len);
                for (int i = 0; i < len; ++i) new_b[i] = and_out[i] ^ NOT_mask;

                return Triple{std::move(min_new_right), std::move(max_new_right), std::move(new_b)};
            });
        }

        tagCursorBase += numBatches * taskStride;

        // 写回右端
        for (int b = 0; b < numBatches; ++b) {
            const int start = b * batchSize;
            const int end = std::min(start + batchSize, totalPairs);
            const int len = end - start;
            if (len <= 0) continue;

            auto triple = futs[b].get();
            auto &min_new_right = std::get<0>(triple);
            auto &max_new_right = std::get<1>(triple);
            auto &new_b = std::get<2>(triple);
            for (int i = 0; i < len; ++i) {
                const int idxR = start + i + delta;
                min_vs[idxR] = min_new_right[i];
                max_vs[idxR] = max_new_right[i];
                bs_bool[idxR] = new_b[i];
            }
        }
    }

    // 组尾：tail[i] = head[i+1]；最后一行 = 1_share
    std::vector<int64_t> group_tails(n);
    for (size_t i = 0; i + 1 < n; ++i) group_tails[i] = heads[i + 1];
    group_tails[n - 1] = Comm::rank();

    // 插入结果列（在 VALID 之前）
    const std::string minOutName = minAlias.empty() ? ("min_" + fieldName) : minAlias;
    const std::string maxOutName = maxAlias.empty() ? ("max_" + fieldName) : maxAlias;

    _fieldNames.reserve(_fieldNames.size() + 2);
    _fieldWidths.reserve(_fieldWidths.size() + 2);
    _dataCols.reserve(_dataCols.size() + 2);

    const int insertPos = colNum() - 2;
    // 先插入min列
    _fieldNames.insert(_fieldNames.begin() + insertPos, minOutName);
    _fieldWidths.insert(_fieldWidths.begin() + insertPos, bitlen);
    _dataCols.insert(_dataCols.begin() + insertPos, std::move(min_vs));

    // 再插入max列（注意插入位置要更新）
    _fieldNames.insert(_fieldNames.begin() + insertPos + 1, maxOutName);
    _fieldWidths.insert(_fieldWidths.begin() + insertPos + 1, bitlen);
    _dataCols.insert(_dataCols.begin() + insertPos + 1, std::move(max_vs));

    _dataCols[colNum() + VALID_COL_OFFSET] = std::move(group_tails);
    clearInvalidEntries(msgTagBase);
}

// Group by functionality for 2PC secret sharing
std::vector<int64_t> View::groupBy(const std::string &groupField, int msgTagBase) {
    size_t n = rowNum();
    if (n == 0) {
        return std::vector<int64_t>();
    }

    // First, sort by the group field to ensure records with same keys are adjacent
    sort(groupField, true, msgTagBase);

    // Choose between single batch and multi-batch processing
    if (Conf::BATCH_SIZE <= 0 || Conf::DISABLE_MULTI_THREAD) {
        return groupBySingleBatch(groupField, msgTagBase);
    } else {
        return groupByMultiBatches(groupField, msgTagBase);
    }
}

// Multi-column group by functionality for 2PC secret sharing
std::vector<int64_t> View::groupBy(const std::vector<std::string> &groupFields, int msgTagBase) {
    size_t n = rowNum();
    if (n == 0 || groupFields.empty()) {
        return std::vector<int64_t>();
    }

    // For single column, use existing implementation
    if (groupFields.size() == 1) {
        return groupBy(groupFields[0], msgTagBase);
    }

    // Step 1: Sort by all group fields to ensure records with same keys are adjacent
    std::vector<bool> ascendingOrders(groupFields.size(), true);
    sort(groupFields, ascendingOrders, msgTagBase);

    // Choose between single batch and multi-batch processing
    if (Conf::BATCH_SIZE <= 0 || Conf::DISABLE_MULTI_THREAD) {
        return groupBySingleBatch(groupFields, msgTagBase);
    } else {
        return groupByMultiBatches(groupFields, msgTagBase);
    }
}

void View::count(std::vector<std::string> &groupFields, std::vector<int64_t> &heads, std::string alias,
                 int msgTagBase) {
    // Remove all columns except the group field and the redundant columns (valid, padding)
    // 2) 在 move 之前缓存 valid/padding 的下标
    const int oldColNum = static_cast<int>(colNum());
    const int validIdx = oldColNum + VALID_COL_OFFSET;
    const int paddingIdx = oldColNum + PADDING_COL_OFFSET;

    // 3) 解析多列分组下标（保持传入顺序）
    std::vector<int> groupIdx;
    groupIdx.reserve(groupFields.size());
    for (const auto &name: groupFields) {
        int idx = colIndex(name);
        groupIdx.push_back(idx);
    }

    // 4) 组装新列集合：groupFields + valid + padding
    const size_t keep = groupIdx.size() + 2;
    std::vector<std::vector<int64_t> > newDataCols;
    std::vector<std::string> newFieldNames;
    std::vector<int> newFieldWidths;
    newDataCols.reserve(keep);
    newFieldNames.reserve(keep);
    newFieldWidths.reserve(keep);

    // 4.1 移动多列分组字段
    for (int gi: groupIdx) {
        newDataCols.push_back(std::move(_dataCols[gi]));
        newFieldNames.push_back(std::move(_fieldNames[gi]));
        newFieldWidths.push_back(_fieldWidths[gi]);
    }

    // 4.2 移动 valid / padding（用之前缓存的旧索引）
    newDataCols.push_back(std::move(_dataCols[validIdx]));
    newDataCols.push_back(std::move(_dataCols[paddingIdx]));
    newFieldNames.emplace_back(VALID_COL_NAME);
    newFieldNames.emplace_back(PADDING_COL_NAME);
    newFieldWidths.push_back(1);
    newFieldWidths.push_back(1);

    // 5) 用新集合替换视图
    _dataCols = std::move(newDataCols);
    _fieldNames = std::move(newFieldNames);
    _fieldWidths = std::move(newFieldWidths);

    if (Conf::BATCH_SIZE <= 0 || Conf::DISABLE_MULTI_THREAD) {
        countSingleBatch(heads, alias, msgTagBase);
    } else {
        countMultiBatches(heads, alias, msgTagBase);
    }
}

void View::max(std::vector<int64_t> &heads, const std::string &fieldName, std::string alias, int msgTagBase) {
    if (Conf::BATCH_SIZE <= 0 || Conf::DISABLE_MULTI_THREAD) {
        maxSingleBatch(heads, fieldName, alias, msgTagBase);
    } else {
        maxMultiBatches(heads, fieldName, alias, msgTagBase);
    }
}

void View::min(std::vector<int64_t> &heads, const std::string &fieldName, std::string alias, int msgTagBase) {
    if (Conf::BATCH_SIZE <= 0 || Conf::DISABLE_MULTI_THREAD) {
        minSingleBatch(heads, fieldName, alias, msgTagBase);
    } else {
        minMultiBatches(heads, fieldName, alias, msgTagBase);
    }
}

void View::minAndMax(std::vector<int64_t> &heads, const std::string &fieldName, std::string minAlias,
                     std::string maxAlias, int msgTagBase) {
    if (Conf::BATCH_SIZE <= 0 || Conf::DISABLE_MULTI_THREAD) {
        minAndMaxSingleBatch(heads, fieldName, minAlias, maxAlias, msgTagBase);
    } else {
        minAndMaxMultiBatches(heads, fieldName, minAlias, maxAlias, msgTagBase);
    }
}

void View::distinctSingleBatch(int msgTagBase) {
    const size_t rn = rowNum();
    const size_t cn = colNum();
    const int validColIndex = cn + VALID_COL_OFFSET;
    std::vector<int64_t> &validCol = _dataCols[validColIndex];

    if (rn == 0) return;

    // 逐列比较：row_i 与 row_{i-1} 是否完全相等（跳过 bucket tag 列）
    std::vector<int64_t> eqs;  // 长度 rn-1，eqs[i-1] 表示第 i 行与第 i-1 行是否相等
    bool hasDataCol = false;

    for (int col = 0; col < static_cast<int>(cn) - 2; ++col) {
        if (StringUtils::hasPrefix(_fieldNames[col], BUCKET_TAG_PREFIX)) {
            continue; // 跳过 tag 列
        }

        hasDataCol = true;

        std::vector<int64_t> prevData, currData;
        prevData.reserve(rn - 1);
        currData.reserve(rn - 1);
        for (size_t i = 1; i < rn; ++i) {
            currData.push_back(_dataCols[col][i]);
            prevData.push_back(_dataCols[col][i - 1]);
        }

        auto eqs_i = BoolEqualBatchOperator(
                &currData, &prevData, _fieldWidths[col],
                /*tid*/0, msgTagBase,
                SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        if (!hasDataCol || eqs.empty()) {
            eqs = std::move(eqs_i);
        } else {
            eqs = BoolAndBatchOperator(
                    &eqs, &eqs_i, /*bitlen=*/1,
                    /*tid*/0, msgTagBase,
                    SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        }
    }

    // new_valid：首行永远有效；其余行为 "与上一行不同" 才有效
    std::vector<int64_t> new_valid(rn, Comm::rank());  // 先全部置 1_share
    if (hasDataCol) {
        for (size_t i = 1; i < rn; ++i) {
            // eqs[i-1] == 1 表示相等；取反得到“不相等”
            new_valid[i] = eqs[i - 1] ^ Comm::rank();
        }
    }

    if (DbConf::DISABLE_COMPACTION) {
        // 不压缩：叠加到当前 valid 上
        auto merged = BoolAndBatchOperator(
                &validCol, &new_valid, /*bitlen=*/1,
                /*tid*/0, msgTagBase,
                SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        validCol.swap(merged);
    } else {
        // 压缩：直接覆盖 valid
        validCol.swap(new_valid);
        clearInvalidEntries(msgTagBase);
    }
}

void View::distinctMultiBatches(int msgTagBase) {
    size_t rn = rowNum();
    size_t cn = colNum();
    int validColIndex = cn + VALID_COL_OFFSET;
    std::vector<int64_t> &validCol = _dataCols[validColIndex];

    // Count valid columns (excluding bucket tag columns)
    std::vector<int> validCols;
    for (int col = 0; col < cn - 2; col++) {
        if (!StringUtils::hasPrefix(_fieldNames[col], BUCKET_TAG_PREFIX)) {
            validCols.push_back(col);
        }
    }

    if (validCols.empty()) {
        // No valid columns to compare, all rows are considered unique
        for (int i = 1; i < rn; i++) {
            validCol[i] = Comm::rank();
        }
        return;
    }

    int batchSize = Conf::BATCH_SIZE;
    int batchNum = ((rn - 1) + batchSize - 1) / batchSize; // Number of batches for row comparisons
    int tagOffset = std::max(BoolEqualBatchOperator::tagStride(), BoolAndBatchOperator::tagStride());

    // Process each column in parallel
    std::vector<std::future<std::vector<int64_t> > > columnFutures(validCols.size());

    for (int colIdx = 0; colIdx < validCols.size(); colIdx++) {
        columnFutures[colIdx] = ThreadPoolSupport::submit([&, colIdx] {
            int col = validCols[colIdx];
            std::vector<int64_t> columnResult;
            columnResult.reserve(rn - 1);

            // Process this column in batches
            std::vector<std::future<std::vector<int64_t> > > batchFutures(batchNum);
            int startTag = tagOffset * batchNum * colIdx;

            for (int b = 0; b < batchNum; ++b) {
                batchFutures[b] = ThreadPoolSupport::submit([&, b, col, startTag]() {
                    int start = b * batchSize + 1; // +1 because we start from row 1
                    int end = std::min(start + batchSize, static_cast<int>(rn));
                    int currentBatchSize = end - start;

                    if (currentBatchSize <= 0) {
                        return std::vector<int64_t>();
                    }

                    std::vector<int64_t> currData, prevData;
                    currData.reserve(currentBatchSize);
                    prevData.reserve(currentBatchSize);

                    for (int i = start; i < end; i++) {
                        currData.push_back(_dataCols[col][i]);
                        prevData.push_back(_dataCols[col][i - 1]);
                    }

                    int batchStartTag = startTag + b * tagOffset;
                    return BoolEqualBatchOperator(&currData, &prevData, _fieldWidths[col],
                                                  0, batchStartTag,
                                                  SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
                });
            }

            // Collect results from all batches for this column
            for (auto &f: batchFutures) {
                auto batchResult = f.get();
                columnResult.insert(columnResult.end(), batchResult.begin(), batchResult.end());
            }

            return columnResult;
        });
    }

    // Collect results from all columns
    std::vector<std::vector<int64_t> > columnResults(validCols.size());
    for (int colIdx = 0; colIdx < validCols.size(); colIdx++) {
        columnResults[colIdx] = columnFutures[colIdx].get();
    }

    // Combine results using AND operation
    std::vector<int64_t> eqs;
    if (!columnResults.empty()) {
        eqs = std::move(columnResults[0]);

        for (int colIdx = 1; colIdx < columnResults.size(); colIdx++) {
            eqs = BoolAndBatchOperator(&eqs, &columnResults[colIdx], 1, 0, msgTagBase,
                                       SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        }
    }

    // Set valid column: if row equals previous row, mark as duplicate (invalid)
    for (int i = 1; i < rn; i++) {
        validCol[i] = eqs[i - 1] ^ Comm::rank(); // XOR with rank to flip: equal=0 (invalid), different=1 (valid)
    }

    clearInvalidEntries(msgTagBase);
}

void View::distinct(int msgTagBase) {
    size_t rn = rowNum();
    if (rn == 0) {
        return;
    }
    size_t cn = colNum();

    // Step 1: Sort all columns to group identical rows together
    // Use all columns for multi-column sorting to ensure identical rows are adjacent
    std::vector<std::string> allFields;
    std::vector<bool> ascendingOrders;

    // Include all data columns (excluding valid and padding columns)
    for (int i = 0; i < cn - 2; i++) {
        if (StringUtils::hasPrefix(_fieldNames[i], BUCKET_TAG_PREFIX)) {
            continue;
        }
        allFields.push_back(_fieldNames[i]);
        ascendingOrders.push_back(true); // Sort in ascending order
    }

    sort(allFields, ascendingOrders, msgTagBase);

    // Step 2: Mark duplicate rows for removal
    // For each row, compare with the previous row to determine if it's a duplicate
    int validColIndex = cn + VALID_COL_OFFSET;
    std::vector<int64_t> &validCol = _dataCols[validColIndex];

    // First row is always unique (not a duplicate)
    validCol[0] = Comm::rank(); // Alice gets 1, Bob gets 0 (XOR = 1 = keep)

    if (rn == 1 && !DbConf::DISABLE_COMPACTION) {
        clearInvalidEntries(msgTagBase);
        return;
    }

    // Choose between single batch and multi-batch processing
    if (Conf::BATCH_SIZE <= 0 || Conf::DISABLE_MULTI_THREAD) {
        distinctSingleBatch(msgTagBase);
    } else {
        distinctMultiBatches(msgTagBase);
    }
}

int View::groupByTagStride() {
    size_t n = rowNum();
    if (n == 0) {
        return 0;
    }

    // Calculate tag stride needed for group by operation
    // Each record needs: comparison, bool-to-arith conversion, arithmetic operations
    int tags_per_record = BoolEqualBatchOperator::tagStride() +
                          BoolToArithBatchOperator::tagStride() +
                          ArithMutexBatchOperator::tagStride(64) +
                          ArithMultiplyBatchOperator::tagStride(64);

    // Add sort tag stride
    int sort_stride = sortTagStride();

    return sort_stride + static_cast<int>(n) * tags_per_record;
}

int View::distinctTagStride() {
    return std::max({
        BoolEqualBatchOperator::tagStride(), BoolAndBatchOperator::tagStride(),
        clearInvalidEntriesTagStride()
    });
}

void View::bitonicSort(const std::string &orderField, bool ascendingOrder, int msgTagBase) {
    if (rowNum() <= 1) {
        return;
    }
    if (Conf::BATCH_SIZE <= 0 || Conf::DISABLE_MULTI_THREAD) {
        bitonicSortSingleBatch(orderField, ascendingOrder, msgTagBase);
    } else {
        bitonicSortMultiBatches(orderField, ascendingOrder, msgTagBase);
    }
}

// Multi-column sort implementation
void View::sort(const std::vector<std::string> &orderFields, const std::vector<bool> &ascendingOrders, int msgTagBase) {
    if (orderFields.empty()) {
        return;
    }

    // For single column, use existing implementation
    if (orderFields.size() == 1) {
        sort(orderFields[0], ascendingOrders[0], msgTagBase);
        return;
    }

    size_t n = rowNum();
    if (n == 0) {
        return;
    }

    bool isPowerOf2 = (n > 0) && ((n & (n - 1)) == 0);
    if (!isPowerOf2) {
        size_t nextPow2 = static_cast<size_t>(1) <<
                          static_cast<size_t>(std::ceil(std::log2(n)));
        // for each column, fill to next 2's pow with 1
        for (auto &v: _dataCols) {
            v.resize(nextPow2, 1);
        }
    }

    bitonicSort(orderFields, ascendingOrders, msgTagBase);

    if (n < _dataCols[0].size()) {
        for (auto &v: _dataCols) {
            v.resize(n);
        }
    }
}

void View::bitonicSort(const std::vector<std::string> &orderFields, const std::vector<bool> &ascendingOrders,
                       int msgTagBase) {
    if (rowNum() <= 1) {
        return;
    }
    if (Conf::BATCH_SIZE <= 0 || Conf::DISABLE_MULTI_THREAD) {
        bitonicSortSingleBatch(orderFields, ascendingOrders, msgTagBase);
    } else {
        bitonicSortMultiBatches(orderFields, ascendingOrders, msgTagBase);
    }
}

int View::sortTagStride(const std::vector<std::string> &orderFields) {
    if (orderFields.empty()) {
        return 0;
    }

    // For single column, use existing calculation
    if (orderFields.size() == 1) {
        return sortTagStride();
    }

    // For multi-column, we need more tags for comparison operations
    // Each comparison involves multiple columns, so multiply by number of columns
    int base_stride = ((rowNum() / 2 + Conf::BATCH_SIZE - 1) / Conf::BATCH_SIZE) * (colNum() - 1) *
                      BoolMutexBatchOperator::tagStride();

    // Additional tags for multi-column comparison
    int multi_col_factor = static_cast<int>(orderFields.size() * 2); // Conservative estimate

    return base_stride * multi_col_factor;
}

void View::bitonicSortSingleBatch(const std::vector<std::string> &orderFields, const std::vector<bool> &ascendingOrders,
                                  int msgTagBase) {
    auto n = _dataCols[0].size();

    std::vector<int> orderFieldIndices(orderFields.size());
    for (size_t i = 0; i < orderFields.size(); i++) {
        orderFieldIndices[i] = colIndex(orderFields[i]);
    }

    auto &paddings = _dataCols[colNum() + PADDING_COL_OFFSET];
    for (int k = 2; k <= n; k *= 2) {
        for (int j = k / 2; j > 0; j /= 2) {
            std::vector<int64_t> xIdx;
            std::vector<int64_t> yIdx;
            std::vector<bool> dirs;
            size_t halfN = n / 2;
            xIdx.reserve(halfN);
            yIdx.reserve(halfN);
            dirs.reserve(halfN);

            for (int i = 0; i < n; i++) {
                int l = i ^ j;
                if (l <= i) {
                    continue;
                }
                bool dir = (i & k) == 0;
                // last col is padding bool
                if (paddings[i] && paddings[l]) {
                    continue;
                }
                if ((paddings[i] && dir) || (paddings[l] && !dir)) {
                    for (auto &v: _dataCols) {
                        std::swap(v[i], v[l]);
                    }
                    continue;
                }
                if (paddings[i] || paddings[l]) {
                    continue;
                }
                xIdx.push_back(i);
                yIdx.push_back(l);
                dirs.push_back(dir);
            }

            size_t comparingCount = xIdx.size();
            if (comparingCount == 0) {
                continue;
            }

            std::vector<int64_t> xs, ys;
            xs.reserve(comparingCount);
            ys.reserve(comparingCount);
            for (auto idx: xIdx) {
                xs.push_back(_dataCols[orderFieldIndices[0]][idx]);
            }
            for (auto idx: yIdx) {
                ys.push_back(_dataCols[orderFieldIndices[0]][idx]);
            }

            // lts shows if X should be before Y
            std::vector<int64_t> lts = ascendingOrders[0]
                                           ? BoolLessBatchOperator(&xs, &ys, _fieldWidths[orderFieldIndices[0]], 0,
                                                                   msgTagBase,
                                                                   SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis
                                           : BoolLessBatchOperator(&ys, &xs, _fieldWidths[orderFieldIndices[0]], 0,
                                                                   msgTagBase,
                                                                   SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
            std::vector<int64_t> eqs = BoolEqualBatchOperator(&xs, &ys, _fieldWidths[orderFieldIndices[0]], 0,
                                                              msgTagBase,
                                                              SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
            for (int i = 1; i < orderFields.size(); i++) {
                xs.clear();
                ys.clear();
                for (auto idx: xIdx) {
                    xs.push_back(_dataCols[orderFieldIndices[i]][idx]);
                }
                for (auto idx: yIdx) {
                    ys.push_back(_dataCols[orderFieldIndices[i]][idx]);
                }
                std::vector<int64_t> lts_i = ascendingOrders[i]
                                                 ? BoolLessBatchOperator(
                                                     &xs, &ys, _fieldWidths[orderFieldIndices[i]], 0,
                                                     msgTagBase,
                                                     SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis
                                                 : BoolLessBatchOperator(
                                                     &ys, &xs, _fieldWidths[orderFieldIndices[i]], 0,
                                                     msgTagBase,
                                                     SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

                // If eqs is true, use lts_i (current column result), otherwise use lts (previous result)
                lts = BoolMutexBatchOperator(&lts_i, &lts, &eqs, 1, 0, msgTagBase,
                                             SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

                std::vector<int64_t> eqs_i = BoolEqualBatchOperator(&xs, &ys, _fieldWidths[orderFieldIndices[i]], 0,
                                                                    msgTagBase,
                                                                    SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

                eqs = BoolAndBatchOperator(&eqs, &eqs_i, 1, 0, msgTagBase, SecureOperator::NO_CLIENT_COMPUTE).execute()
                        ->_zis;
            }


            for (int i = 0; i < comparingCount; i++) {
                if (!dirs[i]) {
                    lts[i] = lts[i] ^ Comm::rank();
                }
            }

            size_t sz = comparingCount * colNum();
            xs.clear();
            ys.clear();
            xs.reserve(sz);
            ys.reserve(sz);

            for (int i = 0; i < colNum() - 1; i++) {
                for (int m = 0; m < comparingCount; m++) {
                    xs.push_back(_dataCols[i][xIdx[m]]);
                    ys.push_back(_dataCols[i][yIdx[m]]);
                }
            }

            auto zs = BoolMutexBatchOperator(&xs, &ys, &lts, _maxWidth, 0, msgTagBase).
                    execute()->_zis;

            int pos = 0;
            for (int i = 0; i < colNum() - 1; i++) {
                auto &co = _dataCols[i];
                for (int m = 0; m < comparingCount; m++) {
                    co[xIdx[m]] = Math::ring(zs[pos * comparingCount + m], _fieldWidths[i]);
                    co[yIdx[m]] = Math::ring(zs[(pos + colNum() - 1) * comparingCount + m], _fieldWidths[i]);
                }
                pos++;
            }
        }
    }
}

void View::bitonicSortMultiBatches(const std::vector<std::string> &orderFields,
                                   const std::vector<bool> &ascendingOrders, int msgTagBase) {
    const int batchSize = Conf::BATCH_SIZE;
    const int n = static_cast<int>(rowNum());

    std::vector<int> orderFieldIndices(orderFields.size());
    for (size_t i = 0; i < orderFields.size(); i++) {
        orderFieldIndices[i] = colIndex(orderFields[i]);
    }

    auto &paddings = _dataCols[colNum() + PADDING_COL_OFFSET];

    for (int k = 2; k <= n; k <<= 1) {
        for (int j = k >> 1; j > 0; j >>= 1) {
            size_t halfN = n / 2;
            std::vector<int> xIdx, yIdx;
            std::vector<bool> dirs;
            xIdx.reserve(halfN);
            yIdx.reserve(halfN);
            dirs.reserve(halfN);

            for (int i = 0; i < n; ++i) {
                int l = i ^ j;
                if (l <= i) {
                    continue;
                }
                bool dir = (i & k) == 0;
                if (paddings[i] && paddings[l]) {
                    continue;
                }
                if ((paddings[i] && dir) || (paddings[l] && !dir)) {
                    for (auto &v: _dataCols) {
                        std::swap(v[i], v[l]);
                    }
                    continue;
                }
                if (paddings[i] || paddings[l]) {
                    continue;
                }
                xIdx.push_back(i);
                yIdx.push_back(l);
                dirs.push_back(dir);
            }

            int comparingCount = static_cast<int>(xIdx.size());
            if (comparingCount == 0) {
                continue;
            }

            const int numBatches = (comparingCount + batchSize - 1) / batchSize;
            // Calculate tag stride considering multi-column comparisons
            const int baseTagStride = BoolLessBatchOperator::tagStride();
            const int eqTagStride = BoolEqualBatchOperator::tagStride();
            const int andTagStride = BoolAndBatchOperator::tagStride();
            const int mutexTagStride = BoolMutexBatchOperator::tagStride();

            // For sequential execution, we only need the maximum tag stride among all operators
            // since tags can be reused for sequential operations
            const int maxOperatorTagStride = std::max({baseTagStride, eqTagStride, andTagStride, mutexTagStride});
            const int tagsPerBatch = maxOperatorTagStride + (colNum() - 1) * mutexTagStride;

            std::vector<std::future<void> > futures;
            futures.reserve(numBatches);

            for (int b = 0; b < numBatches; ++b) {
                futures.emplace_back(ThreadPoolSupport::submit([&, b]() {
                    const int start = b * batchSize;
                    const int end = std::min(start + batchSize, comparingCount);
                    const int cnt = end - start;
                    const int batchTagBase = msgTagBase + tagsPerBatch * b;
                    int currentTag = batchTagBase;

                    // Multi-column comparison logic
                    std::vector<int64_t> xs, ys;
                    xs.reserve(cnt);
                    ys.reserve(cnt);

                    // Start with first column comparison
                    for (int t = start; t < end; ++t) {
                        xs.push_back(_dataCols[orderFieldIndices[0]][xIdx[t]]);
                        ys.push_back(_dataCols[orderFieldIndices[0]][yIdx[t]]);
                    }

                    // lts shows if X should be before Y for first column
                    std::vector<int64_t> lts = ascendingOrders[0]
                                                   ? BoolLessBatchOperator(
                                                       &xs, &ys, _fieldWidths[orderFieldIndices[0]], 0,
                                                       batchTagBase,
                                                       SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis
                                                   : BoolLessBatchOperator(
                                                       &ys, &xs, _fieldWidths[orderFieldIndices[0]], 0,
                                                       batchTagBase,
                                                       SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

                    std::vector<int64_t> eqs = BoolEqualBatchOperator(&xs, &ys, _fieldWidths[orderFieldIndices[0]], 0,
                                                                      batchTagBase,
                                                                      SecureOperator::NO_CLIENT_COMPUTE).execute()->
                            _zis;

                    // Process remaining columns - reuse the same tag base since operations are sequential
                    for (int col = 1; col < orderFields.size(); col++) {
                        xs.clear();
                        ys.clear();
                        for (int t = start; t < end; ++t) {
                            xs.push_back(_dataCols[orderFieldIndices[col]][xIdx[t]]);
                            ys.push_back(_dataCols[orderFieldIndices[col]][yIdx[t]]);
                        }

                        std::vector<int64_t> lts_i = ascendingOrders[col]
                                                         ? BoolLessBatchOperator(
                                                             &xs, &ys, _fieldWidths[orderFieldIndices[col]], 0,
                                                             batchTagBase,
                                                             SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis
                                                         : BoolLessBatchOperator(
                                                             &ys, &xs, _fieldWidths[orderFieldIndices[col]], 0,
                                                             batchTagBase,
                                                             SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

                        // If eqs is true, use lts_i (current column result), otherwise use lts (previous result)
                        lts = BoolMutexBatchOperator(&lts_i, &lts, &eqs, 1, 0, batchTagBase,
                                                     SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

                        std::vector<int64_t> eqs_i = BoolEqualBatchOperator(
                            &xs, &ys, _fieldWidths[orderFieldIndices[col]], 0,
                            batchTagBase,
                            SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

                        eqs = BoolAndBatchOperator(&eqs, &eqs_i, 1, 0, batchTagBase,
                                                   SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
                    }

                    // Apply direction
                    for (int t = 0; t < cnt; ++t) {
                        if (!dirs[start + t]) {
                            lts[t] ^= Comm::rank();
                        }
                    }

                    // Parallel process each column (except padding column)
                    std::vector<std::future<void> > futures2;
                    futures2.reserve(colNum() - 1);

                    for (int c = 0; c < colNum() - 1; ++c) {
                        futures2.push_back(ThreadPoolSupport::submit([&, b, c, currentTag, start, end, cnt] {
                            auto &col = _dataCols[c];
                            std::vector<int64_t> subXs, subYs;
                            subXs.reserve(cnt);
                            subYs.reserve(cnt);

                            for (int t = start; t < end; ++t) {
                                subXs.push_back(col[xIdx[t]]);
                                subYs.push_back(col[yIdx[t]]);
                            }

                            auto copy = lts;
                            auto zs2 = BoolMutexBatchOperator(&subXs, &subYs, &copy, _fieldWidths[c], 0,
                                                              currentTag + mutexTagStride * c).execute()->_zis;

                            for (int t = 0; t < cnt; ++t) {
                                col[xIdx[start + t]] = zs2[t];
                                col[yIdx[start + t]] = zs2[cnt + t];
                            }
                        }));
                    }

                    for (auto &f: futures2) {
                        f.wait();
                    }
                }));
            }

            for (auto &f: futures) {
                f.wait();
            }
        }
    }
}
