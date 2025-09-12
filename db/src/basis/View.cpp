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
    const size_t n = rowNum();
    if (n == 0) return {};

    const int k = colIndex(groupField);
    if (k < 0) {
        Log::e("groupBySingleBatch: field '{}' not found", groupField);
        return std::vector<int64_t>(n, 0);
    }

    auto &keys = _dataCols[k];
    const int bw = _fieldWidths[k];

    // 计算相邻是否相等：eqs[i-1] = (key[i] == key[i-1]), 长度 n-1
    std::vector<int64_t> eqs; // Bool shares
    if (n > 1) {
        std::vector<int64_t> curr, prev;
        curr.reserve(n - 1);
        prev.reserve(n - 1);
        for (size_t i = 1; i < n; ++i) {
            curr.push_back(keys[i]);
            prev.push_back(keys[i - 1]);
        }
        eqs = BoolEqualBatchOperator(&curr, &prev, bw, /*tid=*/0, msgTagBase,
                                     SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    }

    const bool precise = (!DbConf::BASELINE_MODE) && (!DbConf::DISABLE_PRECISE_COMPACTION);

    std::vector<int64_t> heads(n, 0);

    if (precise) {
        // 精确压缩：通常已不存在无效行
        heads[0] = Comm::rank(); // 1_share
        if (n > 1) {
            // NOT(eqs)
            for (size_t i = 1; i < n; ++i) {
                heads[i] = eqs[i - 1] ^ Comm::rank();
            }
        }
        return heads;
    }

    // BASELINE 或 非精确压缩：用 valid 做门控
    const int validIdx = colNum() + VALID_COL_OFFSET;
    auto &validCol = _dataCols[validIdx];

    // head[0] = valid[0]
    heads[0] = (n > 0 ? validCol[0] : 0);

    if (n > 1) {
        // not_eq = NOT(eqs)
        std::vector<int64_t> neq(n - 1);
        for (size_t i = 0; i < n - 1; ++i) neq[i] = eqs[i] ^ Comm::rank();

        // valid_tail = valid[1..n-1]
        std::vector<int64_t> valid_tail(n - 1);
        for (size_t i = 1; i < n; ++i) valid_tail[i - 1] = validCol[i];

        // heads_tail = valid_tail AND not_eq
        auto heads_tail = BoolAndBatchOperator(&valid_tail, &neq, /*bitlen=*/1,
                                               /*tid=*/0, msgTagBase,
                                               SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        for (size_t i = 1; i < n; ++i) heads[i] = heads_tail[i - 1];
    }

    return heads;
}

std::vector<int64_t> View::groupByMultiBatches(const std::string &groupField, int msgTagBase) {
    const size_t n = rowNum();
    if (n == 0) return {};

    if (n <= static_cast<size_t>(Conf::BATCH_SIZE)) {
        return groupBySingleBatch(groupField, msgTagBase);
    }

    const int k = colIndex(groupField);
    if (k < 0) {
        Log::e("groupByMultiBatches: field '{}' not found", groupField);
        return std::vector<int64_t>(n, 0);
    }
    auto &keys = _dataCols[k];
    const int bw = _fieldWidths[k];

    // —— 计算相邻是否相等：eqs[i-1] = (key[i] == key[i-1])，长度 n-1（分批）
    std::vector<int64_t> eqs;
    eqs.reserve(n > 0 ? n - 1 : 0);
    if (n > 1) {
        const int batchSize = Conf::BATCH_SIZE;
        const int cmpPairs = static_cast<int>(n - 1);
        const int batchNum = (cmpPairs + batchSize - 1) / batchSize;
        const int tagOffset = BoolEqualBatchOperator::tagStride();

        std::vector<std::future<std::vector<int64_t> > > batchFuts(batchNum);
        for (int b = 0; b < batchNum; ++b) {
            batchFuts[b] = ThreadPoolSupport::submit([&, b]() {
                const int start = b * batchSize + 1; // 从第1行开始比较 (i 与 i-1)
                const int end = std::min(start + batchSize, static_cast<int>(n));
                const int curN = end - start;
                if (curN <= 0) return std::vector<int64_t>();

                std::vector<int64_t> curr, prev;
                curr.reserve(curN);
                prev.reserve(curN);
                for (int i = start; i < end; ++i) {
                    curr.push_back(keys[i]);
                    prev.push_back(keys[i - 1]);
                }
                const int batchTag = msgTagBase + b * tagOffset;
                return BoolEqualBatchOperator(&curr, &prev, bw, /*tid=*/0, batchTag,
                                              SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
            });
        }
        for (auto &f: batchFuts) {
            auto part = f.get();
            eqs.insert(eqs.end(), part.begin(), part.end());
        }
    }

    const bool precise = !DbConf::DISABLE_PRECISE_COMPACTION; // 不考虑 BASELINE
    std::vector<int64_t> heads(n, 0);

    if (precise) {
        // —— 精确压缩：通常无无效行
        heads[0] = Comm::rank(); // 1_share
        if (n > 1) {
            for (size_t i = 1; i < n; ++i) {
                heads[i] = eqs[i - 1] ^ Comm::rank(); // NOT(eqs)
            }
        }
        return heads;
    }

    // —— 非精确压缩：用 valid 门控
    const int validIdx = colNum() + VALID_COL_OFFSET;
    auto &validCol = _dataCols[validIdx];

    // head[0] = valid[0]
    heads[0] = validCol[0];

    if (n > 1) {
        // not_eq = NOT(eqs)
        std::vector<int64_t> neq(n - 1);
        for (size_t i = 0; i < n - 1; ++i) neq[i] = eqs[i] ^ Comm::rank();

        // valid_tail = valid[1..n-1]
        std::vector<int64_t> valid_tail(n - 1);
        for (size_t i = 1; i < n; ++i) valid_tail[i - 1] = validCol[i];

        // heads_tail = valid_tail AND not_eq
        auto heads_tail = BoolAndBatchOperator(&valid_tail, &neq, /*bitlen=*/1,
                                               /*tid=*/0, msgTagBase,
                                               SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        for (size_t i = 1; i < n; ++i) heads[i] = heads_tail[i - 1];
    }

    return heads;
}

std::vector<int64_t> View::groupBySingleBatch(const std::vector<std::string> &groupFields, int msgTagBase) {
    const size_t n = rowNum();
    std::vector<int64_t> groupHeads(n, 0);
    if (n == 0 || groupFields.empty()) return groupHeads;

    // 找到各分组列索引
    std::vector<int> idx(groupFields.size());
    for (size_t k = 0; k < groupFields.size(); ++k) {
        idx[k] = colIndex(groupFields[k]);
        if (idx[k] < 0) {
            // 任一列不存在则返回全 0（安全起见）
            return groupHeads;
        }
    }

    // 计算相邻行在“所有分组列上”的相等性：eq_all[i-1] = (key[i] == key[i-1])
    std::vector<int64_t> eq_all; // 长度 n-1
    if (n > 1) {
        // 先用第一列初始化
        {
            std::vector<int64_t> cur(n - 1), prv(n - 1);
            for (size_t i = 1; i < n; ++i) {
                cur[i - 1] = _dataCols[idx[0]][i];
                prv[i - 1] = _dataCols[idx[0]][i - 1];
            }
            eq_all = BoolEqualBatchOperator(&cur, &prv, _fieldWidths[idx[0]],
                                            /*tid=*/0, msgTagBase,
                                            SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        }
        // 其余列与 eq_all 做 AND 归并
        for (size_t k = 1; k < idx.size(); ++k) {
            std::vector<int64_t> cur(n - 1), prv(n - 1);
            for (size_t i = 1; i < n; ++i) {
                cur[i - 1] = _dataCols[idx[k]][i];
                prv[i - 1] = _dataCols[idx[k]][i - 1];
            }
            auto eq_k = BoolEqualBatchOperator(&cur, &prv, _fieldWidths[idx[k]],
                                               /*tid=*/0, msgTagBase,
                                               SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
            eq_all = BoolAndBatchOperator(&eq_all, &eq_k, /*bitlen=*/1,
                                          /*tid=*/0, msgTagBase,
                                          SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        }
    }

    const bool precise = !DbConf::DISABLE_PRECISE_COMPACTION;
    const int validColIndex = colNum() + VALID_COL_OFFSET;
    const auto &validCol = _dataCols[validColIndex];
    const int64_t NOT_mask = Comm::rank();

    if (precise) {
        // —— 精确压缩：没有无效行
        groupHeads[0] = Comm::rank(); // 1_share
        for (size_t i = 1; i < n; ++i) {
            // head[i] = NOT(eq_all[i-1])
            groupHeads[i] = (i - 1 < eq_all.size()) ? (eq_all[i - 1] ^ NOT_mask) : Comm::rank();
        }
    } else {
        // —— BASELINE / 非精确压缩：可能存在无效行（但已按 VALID 降序排过）
        // head[0] = valid[0]
        groupHeads[0] = validCol[0];
        if (n > 1) {
            // head[i] = valid[i] AND NOT(eq_all[i-1])
            std::vector<int64_t> neq(n - 1);
            for (size_t i = 1; i < n; ++i) {
                neq[i - 1] = eq_all[i - 1] ^ NOT_mask;
            }
            // 将 valid[i] 下采样为长度 n-1 的向量，与 not_eq 做按位 AND，最后写回到 groupHeads[i]
            std::vector<int64_t> valid_tail(n - 1);
            for (size_t i = 1; i < n; ++i) valid_tail[i - 1] = validCol[i];

            auto and_res = BoolAndBatchOperator(&valid_tail, &neq, /*bitlen=*/1,
                                                /*tid=*/0, msgTagBase,
                                                SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
            for (size_t i = 1; i < n; ++i) groupHeads[i] = and_res[i - 1];
        }
    }

    return groupHeads;
}

std::vector<int64_t> View::groupByMultiBatches(const std::vector<std::string> &groupFields, int msgTagBase) {
    const size_t n = rowNum();
    if (n == 0) return {};
    if (n <= static_cast<size_t>(Conf::BATCH_SIZE)) {
        return groupBySingleBatch(groupFields, msgTagBase);
    }

    // 取各分组列下标
    std::vector<int> gIdx(groupFields.size());
    for (size_t k = 0; k < groupFields.size(); ++k) {
        gIdx[k] = colIndex(groupFields[k]);
    }

    // 结果：groupHeads，长度 n
    std::vector<int64_t> groupHeads(n, 0);
    // eq_all: 长度 n-1，eq_all[j] 表示 (row j+1) 与 (row j) 在所有分组列上是否相等
    std::vector<int64_t> eq_all;
    eq_all.reserve(n > 0 ? n - 1 : 0);

    if (n > 1) {
        const int batchSize = Conf::BATCH_SIZE;
        const int pairCnt = static_cast<int>(n - 1);
        const int batchNum = (pairCnt + batchSize - 1) / batchSize;
        const int tagOffset = std::max(BoolEqualBatchOperator::tagStride(), BoolAndBatchOperator::tagStride());

        // 每一列并行：对相邻行做相等比较，得到 eqs_col（长度 n-1）
        std::vector<std::future<std::vector<int64_t> > > colFuts(groupFields.size());
        for (int colIdx = 0; colIdx < static_cast<int>(groupFields.size()); ++colIdx) {
            colFuts[colIdx] = ThreadPoolSupport::submit([&, colIdx] {
                const int fidx = gIdx[colIdx];
                std::vector<int64_t> colEq;
                colEq.reserve(n - 1);

                // 该列按 batch 并行
                std::vector<std::future<std::vector<int64_t> > > batchFuts(batchNum);
                const int startTag = tagOffset * batchNum * colIdx;

                for (int b = 0; b < batchNum; ++b) {
                    batchFuts[b] = ThreadPoolSupport::submit([&, b, fidx, startTag]() {
                        const int start = b * batchSize + 1; // 与前一行比较，所以从 1 开始
                        const int end = std::min(start + batchSize, static_cast<int>(n));
                        const int curN = end - start;
                        if (curN <= 0) return std::vector<int64_t>();

                        std::vector<int64_t> curr(curN), prev(curN);
                        for (int i = 0; i < curN; ++i) {
                            curr[i] = _dataCols[fidx][start + i];
                            prev[i] = _dataCols[fidx][start + i - 1];
                        }
                        const int batchTag = startTag + b * tagOffset;
                        return BoolEqualBatchOperator(
                            &curr, &prev, _fieldWidths[fidx],
                            /*tid*/0, batchTag,
                            SecureOperator::NO_CLIENT_COMPUTE
                        ).execute()->_zis;
                    });
                }

                // 拼回该列的 eqs_col
                for (auto &bf: batchFuts) {
                    auto part = bf.get();
                    colEq.insert(colEq.end(), part.begin(), part.end());
                }
                return colEq;
            });
        }

        // 汇总每列的 eqs，用 AND 叠合成 eq_all
        std::vector<std::vector<int64_t> > allCols(groupFields.size());
        for (int colIdx = 0; colIdx < static_cast<int>(groupFields.size()); ++colIdx) {
            allCols[colIdx] = colFuts[colIdx].get();
        }

        if (!allCols.empty()) {
            eq_all = std::move(allCols[0]);
            for (int colIdx = 1; colIdx < static_cast<int>(allCols.size()); ++colIdx) {
                eq_all = BoolAndBatchOperator(
                    &eq_all, &allCols[colIdx], /*bitlen=*/1,
                    /*tid*/0, msgTagBase,
                    SecureOperator::NO_CLIENT_COMPUTE
                ).execute()->_zis;
            }
        }
    }

    const int64_t NOT_mask = Comm::rank();

    if (!DbConf::DISABLE_PRECISE_COMPACTION) {
        // —— 精确压缩：所有行均有效
        groupHeads[0] = Comm::rank(); // 第一行必为组首
        for (size_t i = 1; i < n; ++i) {
            groupHeads[i] = (i - 1 < eq_all.size()) ? (eq_all[i - 1] ^ NOT_mask) : Comm::rank();
        }
    } else {
        // —— 非精确压缩：用 valid 做门控
        const int validColIndex = colNum() + VALID_COL_OFFSET;
        auto &validCol = _dataCols[validColIndex];

        // head[0] = valid[0]
        groupHeads[0] = (n > 0 ? validCol[0] : 0);

        if (n > 1) {
            // not_eq = NOT(eq_all)
            std::vector<int64_t> neq(eq_all.size());
            for (size_t j = 0; j < eq_all.size(); ++j) neq[j] = eq_all[j] ^ NOT_mask;

            // valid_tail[j] = valid[j+1]
            std::vector<int64_t> valid_tail(eq_all.size());
            for (size_t j = 0; j < eq_all.size(); ++j) valid_tail[j] = validCol[j + 1];

            // tail_heads = valid_tail AND not_eq
            auto tail_heads = BoolAndBatchOperator(
                &valid_tail, &neq, /*bitlen=*/1,
                /*tid*/0, msgTagBase,
                SecureOperator::NO_CLIENT_COMPUTE
            ).execute()->_zis;

            // 写回 head[1..]
            for (size_t i = 1; i < n; ++i) {
                groupHeads[i] = tail_heads[i - 1];
            }
        }
    }

    return groupHeads;
}

void View::countSingleBatch(std::vector<int64_t> &heads, std::string alias, int msgTagBase) {
    const size_t n = rowNum();
    if (n == 0) return;

    // 模式配置
    const bool BASELINE = DbConf::BASELINE_MODE;
    const bool APPROX_COMPACT = DbConf::DISABLE_PRECISE_COMPACTION;
    const int validIdx = colNum() + VALID_COL_OFFSET;
    const int64_t rank = Comm::rank();

    // 组头（布尔分享）
    std::vector<int64_t> bs_bool = heads;

    // 在不压缩/近似压缩下，组头仅对有效行生效
    if (BASELINE || APPROX_COMPACT) {
        bs_bool = BoolAndBatchOperator(&bs_bool, &_dataCols[validIdx], 1, 0, msgTagBase,
                                       SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    }

    // 初始化计数值：每个有效行计为1
    std::vector<int64_t> counts(n, 0);
    if (BASELINE || APPROX_COMPACT) {
        // 不压缩/近似压缩：只对有效行计数
        for (size_t i = 0; i < n; ++i) {
            counts[i] = _dataCols[validIdx][i]; // valid列本身就是布尔分享
        }
    } else {
        // 精确压缩：所有行都有效，每行计为1
        for (size_t i = 0; i < n; ++i) {
            counts[i] = rank; // 1_share
        }
    }

    // 转换为算术域进行累加
    auto counts_arith = BoolToArithBatchOperator(&counts, 64, 0, msgTagBase,
                                                 SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

    // 组内前缀和：使用修正的前缀扫描算法
    auto bs_arith = BoolToArithBatchOperator(&bs_bool, 64, 0, msgTagBase,
                                             SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

    for (int delta = 1; delta < static_cast<int>(n); delta <<= 1) {
        const int m = static_cast<int>(n) - delta;
        if (m <= 0) break;

        // 计算传播掩码：如果当前位置不是组头，则累加前面的值
        std::vector<int64_t> propagate_mask(m);
        for (int i = 0; i < m; ++i) {
            // 如果bs_bool[i+delta] == 0（不是组头），则传播；否则不传播
            propagate_mask[i] = rank - bs_arith[i + delta]; // 1 - bs_arith[i+delta]
        }

        // 计算要累加的值
        std::vector<int64_t> add_values(m);
        for (int i = 0; i < m; ++i) {
            add_values[i] = counts_arith[i];
        }

        // 计算传播的增量：propagate_mask * add_values
        auto increments = ArithMultiplyBatchOperator(&propagate_mask, &add_values, 64, 0, msgTagBase,
                                                     SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        // 累加到目标位置
        for (int i = 0; i < m; ++i) {
            counts_arith[i + delta] += increments[i];
        }

        // 更新组头标记：传播OR操作
        std::vector<int64_t> not_l(m), not_r(m);
        for (int i = 0; i < m; ++i) {
            not_l[i] = bs_bool[i] ^ rank;
            not_r[i] = bs_bool[i + delta] ^ rank;
        }
        auto and_out = BoolAndBatchOperator(&not_l, &not_r, 1, 0, msgTagBase,
                                            SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        for (int i = 0; i < m; ++i) {
            bs_bool[i + delta] = and_out[i] ^ rank;
        }

        // 更新算术域的组头标记
        bs_arith = BoolToArithBatchOperator(&bs_bool, 64, 0, msgTagBase,
                                            SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    }

    // 转换回布尔域
    auto counts_bool = ArithToBoolBatchOperator(&counts_arith, 64, 0, msgTagBase,
                                                SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

    // 构造组尾标记
    std::vector<int64_t> group_tails(n);
    for (size_t i = 0; i + 1 < n; ++i) {
        group_tails[i] = heads[i + 1];
    }

    // 处理最后一行和无效区边界
    if (BASELINE || APPROX_COMPACT) {
        auto &valid = _dataCols[validIdx];
        std::vector<int64_t> next_valid(n);
        for (size_t i = 0; i + 1 < n; ++i) {
            next_valid[i] = valid[i + 1];
        }
        next_valid[n - 1] = 0; // 0_share

        // 检测有效区到无效区的边界
        std::vector<int64_t> not_next(n);
        for (size_t i = 0; i < n; ++i) {
            not_next[i] = next_valid[i] ^ rank;
        }
        auto boundary = BoolAndBatchOperator(&valid, &not_next, 1, 0, msgTagBase,
                                             SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        // 组尾 = 原组尾 OR 边界
        std::vector<int64_t> not_gt(n), not_bd(n);
        for (size_t i = 0; i < n; ++i) {
            not_gt[i] = group_tails[i] ^ rank;
            not_bd[i] = boundary[i] ^ rank;
        }
        auto and_not = BoolAndBatchOperator(&not_gt, &not_bd, 1, 0, msgTagBase,
                                            SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        for (size_t i = 0; i < n; ++i) {
            group_tails[i] = and_not[i] ^ rank;
        }
    } else {
        group_tails[n - 1] = rank; // 精确压缩：最后一行是组尾
    }

    // 插入计数列
    const int insertPos = colNum() - 2;
    _fieldNames.insert(_fieldNames.begin() + insertPos, alias.empty() ? COUNT_COL_NAME : alias);
    _fieldWidths.insert(_fieldWidths.begin() + insertPos, 64);
    _dataCols.insert(_dataCols.begin() + insertPos, std::move(counts_bool));

    // 更新valid列并压缩
    const int newValidIdx = colNum() + VALID_COL_OFFSET;
    if (BASELINE) {
        // 不压缩：valid := valid ∧ group_tails
        auto new_valid = BoolAndBatchOperator(&_dataCols[newValidIdx], &group_tails, 1, 0, msgTagBase,
                                              SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        _dataCols[newValidIdx] = std::move(new_valid);
    } else if (APPROX_COMPACT) {
        // 非精确压缩：valid := valid ∧ group_tails，然后近似截断
        auto new_valid = BoolAndBatchOperator(&_dataCols[newValidIdx], &group_tails, 1, 0, msgTagBase,
                                              SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        _dataCols[newValidIdx] = std::move(new_valid);
        clearInvalidEntries(msgTagBase);
    } else {
        // 精确压缩：直接设置valid为组尾并精确截断
        _dataCols[newValidIdx] = std::move(group_tails);
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

    // 模式配置
    const bool BASELINE = DbConf::BASELINE_MODE;
    const bool APPROX_COMPACT = DbConf::DISABLE_PRECISE_COMPACTION;
    const int validIdx = colNum() + VALID_COL_OFFSET;
    const int64_t rank = Comm::rank();

    // 组头（布尔分享）
    std::vector<int64_t> bs_bool = heads;

    // 在不压缩/近似压缩下，组头仅对有效行生效
    if (BASELINE || APPROX_COMPACT) {
        bs_bool = BoolAndBatchOperator(&bs_bool, &_dataCols[validIdx], 1, 0, msgTagBase,
                                       SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    }

    // 初始化计数值：每个有效行计为1
    std::vector<int64_t> counts(n, 0);
    if (BASELINE || APPROX_COMPACT) {
        // 不压缩/近似压缩：只对有效行计数
        for (size_t i = 0; i < n; ++i) {
            counts[i] = _dataCols[validIdx][i]; // valid列本身就是布尔分享
        }
    } else {
        // 精确压缩：所有行都有效，每行计为1
        for (size_t i = 0; i < n; ++i) {
            counts[i] = rank; // 1_share
        }
    }

    // 转换为算术域进行累加
    auto counts_arith = BoolToArithBatchOperator(&counts, 64, 0, msgTagBase,
                                                 SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

    // 组内前缀和：使用修正的前缀扫描算法（并行版本）
    auto bs_arith = BoolToArithBatchOperator(&bs_bool, 64, 0, msgTagBase,
                                             SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

    // 各算子 stride
    const int mulStride = ArithMultiplyBatchOperator::tagStride(64);
    const int andStride = BoolAndBatchOperator::tagStride();
    const int b2aStride = BoolToArithBatchOperator::tagStride();
    const int a2bStride = ArithToBoolBatchOperator::tagStride(64);
    const int taskStride = mulStride + andStride + b2aStride;

    int tagCursorBase = msgTagBase;

    // 并行前缀扫描
    for (int delta = 1; delta < static_cast<int>(n); delta <<= 1) {
        const int totalPairs = static_cast<int>(n) - delta;
        if (totalPairs <= 0) break;

        const int numBatches = (totalPairs + batchSize - 1) / batchSize;

        using Triple = std::tuple<
            std::vector<int64_t>, // increments (len)
            std::vector<int64_t>, // new_bs_bool (len) for [start+delta .. end-1+delta]
            std::vector<int64_t>  // new_bs_arith (len) same range
        >;

        std::vector<std::future<Triple>> futs(numBatches);

        for (int b = 0; b < numBatches; ++b) {
            const int start = b * batchSize;
            const int end   = std::min(start + batchSize, totalPairs);
            const int len   = end - start;

            const int baseTag = tagCursorBase + b * taskStride;
            const int mulTag  = baseTag;
            const int andTag  = baseTag + mulStride;
            const int b2aTag  = baseTag + mulStride + andStride;

            futs[b] = ThreadPoolSupport::submit([=, &counts_arith, &bs_bool, &bs_arith]() -> Triple {
                if (len <= 0) return Triple{{}, {}, {}};

                // 1) 计算传播掩码：如果当前位置不是组头，则累加前面的值
                std::vector<int64_t> propagate_mask(len);
                for (int i = 0; i < len; ++i) {
                    const int idx = start + i;
                    // 如果bs_arith[idx+delta] == 0（不是组头），则传播；否则不传播
                    propagate_mask[i] = rank - bs_arith[idx + delta]; // 1 - bs_arith[idx+delta]
                }

                // 2) 计算要累加的值
                std::vector<int64_t> add_values(len);
                for (int i = 0; i < len; ++i) {
                    const int idx = start + i;
                    add_values[i] = counts_arith[idx];
                }

                // 3) 计算传播的增量：propagate_mask * add_values
                auto increments = ArithMultiplyBatchOperator(&propagate_mask, &add_values, 64, 0, mulTag,
                                                             SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

                // 4) 更新组头标记：传播OR操作
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
                    new_bs_bool[i] = and_res[i] ^ rank; // 取反得到 OR
                }

                // 5) b->a，更新算术域的组头标记
                auto new_bs_arith = BoolToArithBatchOperator(&new_bs_bool, 64, 0, b2aTag,
                                                             SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

                return Triple{std::move(increments), std::move(new_bs_bool), std::move(new_bs_arith)};
            });
        }

        // 消费这一波任务的 tag 空间
        tagCursorBase += numBatches * taskStride;

        // 回填：counts_arith[i+delta]、bs_bool[i+delta]、bs_arith[i+delta]
        for (int b = 0; b < numBatches; ++b) {
            const int start = b * batchSize;
            const int end   = std::min(start + batchSize, totalPairs);
            const int len   = end - start;
            if (len <= 0) continue;

            auto triple       = futs[b].get();
            auto &increments  = std::get<0>(triple);
            auto &new_b_bool  = std::get<1>(triple);
            auto &new_b_arith = std::get<2>(triple);

            for (int i = 0; i < len; ++i) {
                counts_arith[start + i + delta] += increments[i];
                bs_bool[start + i + delta]  = new_b_bool[i];
                bs_arith[start + i + delta] = new_b_arith[i];
            }
        }
    }

    // 转换回布尔域（并行）
    std::vector<int64_t> counts_bool;
    {
        const int numBatches = (static_cast<int>(n) + batchSize - 1) / batchSize;
        std::vector<std::future<std::vector<int64_t>>> futs(numBatches);
        for (int b = 0; b < numBatches; ++b) {
            const int start = b * batchSize;
            const int end   = std::min(start + batchSize, static_cast<int>(n));
            const int len   = end - start;

            if (len <= 0) {
                futs[b] = ThreadPoolSupport::submit([=]() { return std::vector<int64_t>(); });
                continue;
            }
            auto slice = std::make_shared<std::vector<int64_t>>(counts_arith.begin() + start, counts_arith.begin() + end);
            const int tg = tagCursorBase + b * a2bStride;
            futs[b] = ThreadPoolSupport::submit([slice, tg]() {
                return ArithToBoolBatchOperator(slice.get(), 64, 0, tg,
                                                SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
            });
        }
        tagCursorBase += numBatches * a2bStride;

        counts_bool.reserve(n);
        for (int b = 0; b < numBatches; ++b) {
            auto part = futs[b].get();
            counts_bool.insert(counts_bool.end(), part.begin(), part.end());
        }
    }

    // 构造组尾标记
    std::vector<int64_t> group_tails(n);
    for (size_t i = 0; i + 1 < n; ++i) {
        group_tails[i] = heads[i + 1];
    }

    // 处理最后一行和无效区边界
    if (BASELINE || APPROX_COMPACT) {
        auto &valid = _dataCols[validIdx];
        std::vector<int64_t> next_valid(n);
        for (size_t i = 0; i + 1 < n; ++i) {
            next_valid[i] = valid[i + 1];
        }
        next_valid[n - 1] = 0; // 0_share

        // 检测有效区到无效区的边界
        std::vector<int64_t> not_next(n);
        for (size_t i = 0; i < n; ++i) {
            not_next[i] = next_valid[i] ^ rank;
        }
        auto boundary = BoolAndBatchOperator(&valid, &not_next, 1, 0, msgTagBase,
                                             SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        // 组尾 = 原组尾 OR 边界
        std::vector<int64_t> not_gt(n), not_bd(n);
        for (size_t i = 0; i < n; ++i) {
            not_gt[i] = group_tails[i] ^ rank;
            not_bd[i] = boundary[i] ^ rank;
        }
        auto and_not = BoolAndBatchOperator(&not_gt, &not_bd, 1, 0, msgTagBase,
                                            SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        for (size_t i = 0; i < n; ++i) {
            group_tails[i] = and_not[i] ^ rank;
        }
    } else {
        group_tails[n - 1] = rank; // 精确压缩：最后一行是组尾
    }

    // 插入计数列
    const int insertPos = colNum() - 2;
    _fieldNames.insert(_fieldNames.begin() + insertPos, alias.empty() ? COUNT_COL_NAME : alias);
    _fieldWidths.insert(_fieldWidths.begin() + insertPos, 64);
    _dataCols.insert(_dataCols.begin() + insertPos, std::move(counts_bool));

    // 更新valid列并压缩
    const int newValidIdx = colNum() + VALID_COL_OFFSET;
    if (BASELINE) {
        // 不压缩：valid := valid ∧ group_tails
        auto new_valid = BoolAndBatchOperator(&_dataCols[newValidIdx], &group_tails, 1, 0, msgTagBase,
                                              SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        _dataCols[newValidIdx] = std::move(new_valid);
    } else if (APPROX_COMPACT) {
        // 非精确压缩：valid := valid ∧ group_tails，然后近似截断
        auto new_valid = BoolAndBatchOperator(&_dataCols[newValidIdx], &group_tails, 1, 0, msgTagBase,
                                              SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        _dataCols[newValidIdx] = std::move(new_valid);
        clearInvalidEntries(msgTagBase);
    } else {
        // 精确压缩：直接设置valid为组尾并精确截断
        _dataCols[newValidIdx] = std::move(group_tails);
        clearInvalidEntries(msgTagBase);
    }
}

void View::maxSingleBatch(std::vector<int64_t> &heads,
                          const std::string &fieldName,
                          std::string alias,
                          int msgTagBase) {
    const size_t n = rowNum();
    if (n == 0) return;

    // —— 列信息
    const int fieldIdx = colIndex(fieldName);
    if (fieldIdx < 0) {
        Log::e("Field '{}' not found for max operation", fieldName);
        return;
    }
    const int bitlen = _fieldWidths[fieldIdx];

    // —— 配置
    const bool BASELINE       = DbConf::BASELINE_MODE;
    const bool APPROX_COMPACT = DbConf::DISABLE_PRECISE_COMPACTION; // 非精确压缩
    const bool PRECISE_COMPACT = !BASELINE && !APPROX_COMPACT;

    // —— 源数据
    std::vector<int64_t> &src = _dataCols[fieldIdx];
    if (src.size() != n || heads.size() != n) {
        Log::e("Size mismatch: fieldData={}, heads={}, n={}", src.size(), heads.size(), n);
        return;
    }

    // —— 工作副本
    std::vector<int64_t> vs = src;         // 值工作列（布尔份额按 bitlen 对待）
    std::vector<int64_t> bs_bool = heads;  // 组头 (XOR 布尔分享)
    const int64_t XOR_MASK = Comm::rank();

    // —— 在不压缩 / 近似压缩下用 valid 掩蔽输入，避免无效行影响比较与传播
    int validIdx = colNum() + VALID_COL_OFFSET;
    if (BASELINE || APPROX_COMPACT) {
        auto &valid = _dataCols[validIdx];

        // heads := heads ∧ valid
        bs_bool = BoolAndBatchOperator(&bs_bool, &valid, 1, 0, msgTagBase,
                                       SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        // vs := valid ? vs : 0
        std::vector<int64_t> zeros(n, 0);
        vs = BoolMutexBatchOperator(&vs, &zeros, &valid, bitlen, 0, msgTagBase,
                                    SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    }

    // —— 分段 max：Hillis–Steele 前缀扫描
    for (int delta = 1; delta < static_cast<int>(n); delta <<= 1) {
        const int m = static_cast<int>(n) - delta;

        // 快照窗口
        std::vector<int64_t> left_vals(m), right_vals(m);
        for (int i = 0; i < m; ++i) {
            left_vals[i]  = vs[i];
            right_vals[i] = vs[i + delta];
        }

        // 不压缩 / 近似压缩：每轮窗口再按窗口内 valid 掩蔽一次
        if (BASELINE || APPROX_COMPACT) {
            auto &valid = _dataCols[validIdx];
            std::vector<int64_t> zeros(m, 0), left_valid(m), right_valid(m);
            for (int i = 0; i < m; ++i) {
                left_valid[i]  = valid[i];
                right_valid[i] = valid[i + delta];
            }
            left_vals = BoolMutexBatchOperator(&left_vals,  &zeros, &left_valid,  bitlen, 0, msgTagBase,
                                               SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
            right_vals= BoolMutexBatchOperator(&right_vals, &zeros, &right_valid, bitlen, 0, msgTagBase,
                                               SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        }

        // cand = max(left,right) : cmp=(left<right); cand=cmp?right:left
        auto cmp = BoolLessBatchOperator(&left_vals, &right_vals, bitlen, 0, msgTagBase,
                                         SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        auto cand = BoolMutexBatchOperator(&right_vals, &left_vals, &cmp, bitlen, 0, msgTagBase,
                                           SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        // gate = ¬b[i+delta]；new_right = gate ? cand : right
        std::vector<int64_t> gate(m);
        for (int i = 0; i < m; ++i) gate[i] = (bs_bool[i + delta] ^ XOR_MASK);
        auto new_right = BoolMutexBatchOperator(&cand, &right_vals, &gate, bitlen, 0, msgTagBase,
                                                SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        // 写回
        for (int i = 0; i < m; ++i) vs[i + delta] = new_right[i];

        // 传播边界：b[i+delta] = b[i] OR b[i+delta] = ¬(¬b[i] ∧ ¬b[i+delta])
        std::vector<int64_t> not_l(m), not_r(m);
        for (int i = 0; i < m; ++i) {
            not_l[i] = bs_bool[i] ^ XOR_MASK;
            not_r[i] = bs_bool[i + delta] ^ XOR_MASK;
        }
        auto and_out = BoolAndBatchOperator(&not_l, &not_r, 1, 0, msgTagBase,
                                            SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        for (int i = 0; i < m; ++i) bs_bool[i + delta] = (and_out[i] ^ XOR_MASK);
    }

    // —— 组尾：tail[i] = head[i+1]
    std::vector<int64_t> group_tails(n);
    for (size_t i = 0; i + 1 < n; ++i) group_tails[i] = heads[i + 1];

    if (PRECISE_COMPACT) {
        // 精确压缩：最后一行必是尾
        group_tails[n - 1] = Comm::rank();
    } else {
        // 基线/非精确压缩：修正“最后一组后面是无效区”
        auto &valid = _dataCols[validIdx];
        std::vector<int64_t> next_valid(n);
        for (size_t i = 0; i + 1 < n; ++i) next_valid[i] = valid[i + 1];
        next_valid[n - 1] = 0; // 0_share

        // last_valid_tail = valid ∧ ¬next_valid
        std::vector<int64_t> not_next(n);
        for (size_t i = 0; i < n; ++i) not_next[i] = next_valid[i] ^ XOR_MASK;
        auto last_valid_tail = BoolAndBatchOperator(&valid, &not_next, 1, 0, msgTagBase,
                                                    SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        // group_tails |= last_valid_tail  （用德摩根实现）
        std::vector<int64_t> not_gt(n), not_lvt(n);
        for (size_t i = 0; i < n; ++i) { not_gt[i] = group_tails[i] ^ XOR_MASK; not_lvt[i] = last_valid_tail[i] ^ XOR_MASK; }
        auto and_not = BoolAndBatchOperator(&not_gt, &not_lvt, 1, 0, msgTagBase,
                                            SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        for (size_t i = 0; i < n; ++i) group_tails[i] = (and_not[i] ^ XOR_MASK);
    }

    // —— 插入 max 列（在 VALID/PADDING 前）
    const std::string outName = alias.empty() ? ("max_" + fieldName) : alias;
    _fieldNames.reserve(_fieldNames.size() + 1);
    _fieldWidths.reserve(_fieldWidths.size() + 1);
    _dataCols.reserve(_dataCols.size() + 1);

    const int insertPos = colNum() - 2;
    _fieldNames.insert(_fieldNames.begin() + insertPos, outName);
    _fieldWidths.insert(_fieldWidths.begin() + insertPos, bitlen);
    _dataCols.insert(_dataCols.begin() + insertPos, vs);

    validIdx = colNum() + VALID_COL_OFFSET;
    // —— 更新 VALID 并（视模式）截断
    if (BASELINE) {
        // 不压缩：valid := valid ∧ group_tails
        auto new_valid = BoolAndBatchOperator(&_dataCols[validIdx], &group_tails, 1, 0, msgTagBase,
                                              SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        _dataCols[validIdx] = std::move(new_valid);
        // 不物理删除；依赖后续算子继续用 valid 过滤
    } else if (APPROX_COMPACT) {
        // 非精确压缩：valid := valid ∧ group_tails；随后近似截断
        auto new_valid = BoolAndBatchOperator(&_dataCols[validIdx], &group_tails, 1, 0, msgTagBase,
                                              SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        _dataCols[validIdx] = std::move(new_valid);
        clearInvalidEntries(msgTagBase);  // 内部走“近似”截断
    } else {
        // 精确压缩：直接把 VALID 设为组尾并精确截断（只保留每组一行）
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

    // —— 配置
    const bool APPROX_COMPACT = DbConf::DISABLE_PRECISE_COMPACTION; // 非精确压缩
    const int64_t rank = Comm::rank();
    const int validIdx = colNum() + VALID_COL_OFFSET;

    // —— 预处理：根据模式准备 bs_bool 与 vs
    // 边界位：默认用传入的 heads；若非精确压缩，则仅让有效行的组头生效
    std::vector<int64_t> bs_bool = heads;
    std::vector<int64_t> vs = src;

    if (APPROX_COMPACT) {
        // 非精确压缩：组头仅对有效行生效，避免无效区干扰
        bs_bool = BoolAndBatchOperator(&bs_bool, &_dataCols[validIdx], 1, 0, msgTagBase,
                                       SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        // vs := valid ? vs : 0（避免无效行的值影响比较）
        std::vector<int64_t> zeros(n, 0);
        vs = BoolMutexBatchOperator(&vs, &zeros, &_dataCols[validIdx], bitlen, 0, msgTagBase,
                                    SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    }

    // 各算子 stride
    const int lessStride = BoolLessBatchOperator::tagStride();
    const int mutexStride = BoolMutexBatchOperator::tagStride();
    const int andStride = BoolAndBatchOperator::tagStride();
    // 每个批任务用到：Less(1) + Mutex(1) + Mutex(1) + AND(1)
    const int taskStride = lessStride + mutexStride + mutexStride + andStride;

    int tagCursorBase = msgTagBase;
    const int64_t NOT_mask = rank;

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

                // 快照左右值
                std::vector<int64_t> left_vals(len), right_vals(len);
                for (int i = 0; i < len; ++i) {
                    const int idxL = start + i;
                    const int idxR = idxL + delta;
                    left_vals[i] = vs[idxL];
                    right_vals[i] = vs[idxR];
                }

                // 非精确压缩：每轮窗口再按窗口内 valid 掩蔽一次
                if (APPROX_COMPACT) {
                    auto &valid = _dataCols[validIdx];
                    std::vector<int64_t> zeros(len, 0), left_valid(len), right_valid(len);
                    for (int i = 0; i < len; ++i) {
                        left_valid[i] = valid[start + i];
                        right_valid[i] = valid[start + i + delta];
                    }
                    left_vals = BoolMutexBatchOperator(&left_vals, &zeros, &left_valid, bitlen, 0, lessTag,
                                                       SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
                    right_vals = BoolMutexBatchOperator(&right_vals, &zeros, &right_valid, bitlen, 0, lessTag,
                                                        SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
                }

                // 1) cmp = (left < right)
                auto cmp = BoolLessBatchOperator(&left_vals, &right_vals, bitlen,
                                                 /*tid*/0, /*tag*/lessTag,
                                                 SecureOperator::NO_CLIENT_COMPUTE)
                        .execute()->_zis;

                // 2) cand = max(left,right) : cand = cmp ? right : left
                auto cand = BoolMutexBatchOperator(&right_vals, &left_vals, &cmp,
                                                   bitlen, /*tid*/0, /*tag*/mtx1Tag,
                                                   SecureOperator::NO_CLIENT_COMPUTE)
                        .execute()->_zis;

                // 3) gate = ~b[i+delta]；new_right = gate ? cand : right
                std::vector<int64_t> not_bR(len);
                for (int i = 0; i < len; ++i) {
                    not_bR[i] = bs_bool[start + i + delta] ^ NOT_mask;
                }
                auto new_right = BoolMutexBatchOperator(&cand, &right_vals, &not_bR,
                                                        bitlen, /*tid*/0, /*tag*/mtx2Tag,
                                                        SecureOperator::NO_CLIENT_COMPUTE)
                        .execute()->_zis;

                // 4) 边界传播：b[i+delta] = b[i] OR b[i+delta] = ~(~b[i] & ~b[i+delta])
                std::vector<int64_t> not_bL(len);
                for (int i = 0; i < len; ++i) {
                    not_bL[i] = bs_bool[start + i] ^ NOT_mask;
                    not_bR[i] = bs_bool[start + i + delta] ^ NOT_mask;
                }
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

    // —— 构造组尾
    std::vector<int64_t> group_tails(n);
    for (size_t i = 0; i + 1 < n; ++i) group_tails[i] = heads[i + 1];

    // 非精确压缩：修正"最后一组尾巴落到无效区"的情况
    if (APPROX_COMPACT) {
        auto &valid = _dataCols[validIdx];
        std::vector<int64_t> next_valid(n);
        for (size_t i = 0; i + 1 < n; ++i) next_valid[i] = valid[i + 1];
        next_valid[n - 1] = 0; // 0_share

        // last_valid_tail = valid ∧ ¬next_valid
        std::vector<int64_t> not_next(n);
        for (size_t i = 0; i < n; ++i) not_next[i] = next_valid[i] ^ rank;

        auto last_valid_tail = BoolAndBatchOperator(&valid, &not_next, 1, 0, msgTagBase,
                                                    SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        // group_tails = group_tails OR last_valid_tail  （德摩根）
        std::vector<int64_t> not_gt(n), not_lvt(n);
        for (size_t i = 0; i < n; ++i) { 
            not_gt[i] = group_tails[i] ^ rank; 
            not_lvt[i] = last_valid_tail[i] ^ rank; 
        }
        auto and_not = BoolAndBatchOperator(&not_gt, &not_lvt, 1, 0, msgTagBase,
                                            SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        for (size_t i = 0; i < n; ++i) group_tails[i] = and_not[i] ^ rank;
    } else {
        group_tails[n - 1] = rank;
    }

    // 插入结果列（位置与单批版一致：在 VALID 之前）
    const std::string outName = alias.empty() ? ("max_" + fieldName) : alias;

    _fieldNames.reserve(_fieldNames.size() + 1);
    _fieldWidths.reserve(_fieldWidths.size() + 1);
    _dataCols.reserve(_dataCols.size() + 1);

    const int insertPos = colNum() - 2;
    _fieldNames.insert(_fieldNames.begin() + insertPos, outName);
    _fieldWidths.insert(_fieldWidths.begin() + insertPos, bitlen);
    _dataCols.insert(_dataCols.begin() + insertPos, std::move(vs));

    // —— 根据模式更新 VALID 并压缩
    if (APPROX_COMPACT) {
        auto new_valid = BoolAndBatchOperator(&_dataCols[validIdx], &group_tails, 1, 0, msgTagBase,
                                              SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        _dataCols[validIdx] = std::move(new_valid);
        clearInvalidEntries(msgTagBase); // 近似截断
    } else {
        _dataCols[validIdx] = std::move(group_tails);
        clearInvalidEntries(msgTagBase); // 精确截断
    }
}

// —— 单批：分段最小值（Hillis–Steele 扫描 + 组边界传播）
void View::minSingleBatch(std::vector<int64_t> &heads,
                          const std::string &fieldName,
                          std::string alias,
                          int msgTagBase) {
    const size_t n = rowNum();
    if (n == 0) return;

    // —— 列信息
    const int fieldIdx = colIndex(fieldName);
    if (fieldIdx < 0) {
        Log::e("Field '{}' not found for min operation", fieldName);
        return;
    }
    const int bitlen = _fieldWidths[fieldIdx];

    // —— 配置
    const bool BASELINE       = DbConf::BASELINE_MODE;
    const bool APPROX_COMPACT = DbConf::DISABLE_PRECISE_COMPACTION; // 非精确压缩
    const bool PRECISE_COMPACT = !BASELINE && !APPROX_COMPACT;

    // —— 源数据
    std::vector<int64_t> &src = _dataCols[fieldIdx];
    if (src.size() != n || heads.size() != n) {
        Log::e("Size mismatch: fieldData={}, heads={}, n={}", src.size(), heads.size(), n);
        return;
    }

    // —— 工作副本
    std::vector<int64_t> vs = src;         // 值工作列（布尔份额按 bitlen 对待）
    std::vector<int64_t> bs_bool = heads;  // 组头 (XOR 布尔分享)
    const int64_t XOR_MASK = Comm::rank();

    // —— 在不压缩 / 近似压缩下用 valid 掩蔽输入，避免无效行影响比较与传播
    int validIdx = colNum() + VALID_COL_OFFSET;
    if (BASELINE || APPROX_COMPACT) {
        auto &valid = _dataCols[validIdx];

        // heads := heads ∧ valid
        bs_bool = BoolAndBatchOperator(&bs_bool, &valid, 1, 0, msgTagBase,
                                       SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        // vs := valid ? vs : MAX_VALUE (对于min，无效行应该设为最大值，这样不会影响最小值计算)
        std::vector<int64_t> max_vals(n, (1LL << (bitlen - 1)) - 1); // 设为该位宽下的最大值
        vs = BoolMutexBatchOperator(&vs, &max_vals, &valid, bitlen, 0, msgTagBase,
                                    SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    }

    // —— 分段 min：Hillis–Steele 前缀扫描
    for (int delta = 1; delta < static_cast<int>(n); delta <<= 1) {
        const int m = static_cast<int>(n) - delta;

        // 快照窗口
        std::vector<int64_t> left_vals(m), right_vals(m);
        for (int i = 0; i < m; ++i) {
            left_vals[i]  = vs[i];
            right_vals[i] = vs[i + delta];
        }

        // 不压缩 / 近似压缩：每轮窗口再按窗口内 valid 掩蔽一次
        if (BASELINE || APPROX_COMPACT) {
            auto &valid = _dataCols[validIdx];
            std::vector<int64_t> max_vals(m, (1LL << (bitlen - 1)) - 1), left_valid(m), right_valid(m);
            for (int i = 0; i < m; ++i) {
                left_valid[i]  = valid[i];
                right_valid[i] = valid[i + delta];
            }
            left_vals = BoolMutexBatchOperator(&left_vals,  &max_vals, &left_valid,  bitlen, 0, msgTagBase,
                                               SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
            right_vals= BoolMutexBatchOperator(&right_vals, &max_vals, &right_valid, bitlen, 0, msgTagBase,
                                               SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        }

        // cand = min(left,right) : cmp=(left<right); cand=cmp?left:right
        auto cmp = BoolLessBatchOperator(&left_vals, &right_vals, bitlen, 0, msgTagBase,
                                         SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        auto cand = BoolMutexBatchOperator(&left_vals, &right_vals, &cmp, bitlen, 0, msgTagBase,
                                           SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        // gate = ¬b[i+delta]；new_right = gate ? cand : right
        std::vector<int64_t> gate(m);
        for (int i = 0; i < m; ++i) gate[i] = (bs_bool[i + delta] ^ XOR_MASK);
        auto new_right = BoolMutexBatchOperator(&cand, &right_vals, &gate, bitlen, 0, msgTagBase,
                                                SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        // 写回
        for (int i = 0; i < m; ++i) vs[i + delta] = new_right[i];

        // 传播边界：b[i+delta] = b[i] OR b[i+delta] = ¬(¬b[i] ∧ ¬b[i+delta])
        std::vector<int64_t> not_l(m), not_r(m);
        for (int i = 0; i < m; ++i) {
            not_l[i] = bs_bool[i] ^ XOR_MASK;
            not_r[i] = bs_bool[i + delta] ^ XOR_MASK;
        }
        auto and_out = BoolAndBatchOperator(&not_l, &not_r, 1, 0, msgTagBase,
                                            SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        for (int i = 0; i < m; ++i) bs_bool[i + delta] = (and_out[i] ^ XOR_MASK);
    }

    // —— 组尾：tail[i] = head[i+1]
    std::vector<int64_t> group_tails(n);
    for (size_t i = 0; i + 1 < n; ++i) group_tails[i] = heads[i + 1];

    if (PRECISE_COMPACT) {
        // 精确压缩：最后一行必是尾
        group_tails[n - 1] = Comm::rank();
    } else {
        // 基线/非精确压缩：修正"最后一组后面是无效区"
        auto &valid = _dataCols[validIdx];
        std::vector<int64_t> next_valid(n);
        for (size_t i = 0; i + 1 < n; ++i) next_valid[i] = valid[i + 1];
        next_valid[n - 1] = 0; // 0_share

        // last_valid_tail = valid ∧ ¬next_valid
        std::vector<int64_t> not_next(n);
        for (size_t i = 0; i < n; ++i) not_next[i] = next_valid[i] ^ XOR_MASK;
        auto last_valid_tail = BoolAndBatchOperator(&valid, &not_next, 1, 0, msgTagBase,
                                                    SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        // group_tails |= last_valid_tail  （用德摩根实现）
        std::vector<int64_t> not_gt(n), not_lvt(n);
        for (size_t i = 0; i < n; ++i) { not_gt[i] = group_tails[i] ^ XOR_MASK; not_lvt[i] = last_valid_tail[i] ^ XOR_MASK; }
        auto and_not = BoolAndBatchOperator(&not_gt, &not_lvt, 1, 0, msgTagBase,
                                            SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        for (size_t i = 0; i < n; ++i) group_tails[i] = (and_not[i] ^ XOR_MASK);
    }

    // —— 插入 min 列（在 VALID/PADDING 前）
    const std::string outName = alias.empty() ? ("min_" + fieldName) : alias;
    _fieldNames.reserve(_fieldNames.size() + 1);
    _fieldWidths.reserve(_fieldWidths.size() + 1);
    _dataCols.reserve(_dataCols.size() + 1);

    const int insertPos = colNum() - 2;
    _fieldNames.insert(_fieldNames.begin() + insertPos, outName);
    _fieldWidths.insert(_fieldWidths.begin() + insertPos, bitlen);
    _dataCols.insert(_dataCols.begin() + insertPos, vs);

    validIdx = colNum() + VALID_COL_OFFSET;
    // —— 更新 VALID 并（视模式）截断
    if (BASELINE) {
        // 不压缩：valid := valid ∧ group_tails
        auto new_valid = BoolAndBatchOperator(&_dataCols[validIdx], &group_tails, 1, 0, msgTagBase,
                                              SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        _dataCols[validIdx] = std::move(new_valid);
        // 不物理删除；依赖后续算子继续用 valid 过滤
    } else if (APPROX_COMPACT) {
        // 非精确压缩：valid := valid ∧ group_tails；随后近似截断
        auto new_valid = BoolAndBatchOperator(&_dataCols[validIdx], &group_tails, 1, 0, msgTagBase,
                                              SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        _dataCols[validIdx] = std::move(new_valid);
        clearInvalidEntries(msgTagBase);  // 内部走"近似"截断
    } else {
        // 精确压缩：直接把 VALID 设为组尾并精确截断（只保留每组一行）
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

    // —— 配置
    const bool APPROX_COMPACT = DbConf::DISABLE_PRECISE_COMPACTION; // 非精确压缩
    const int64_t rank = Comm::rank();
    int validIdx = colNum() + VALID_COL_OFFSET;

    // —— 预处理：根据模式准备 bs_bool 与 vs
    // 边界位：默认用传入的 heads；若非精确压缩，则仅让有效行的组头生效
    std::vector<int64_t> bs_bool = heads;
    std::vector<int64_t> vs = src;

    if (APPROX_COMPACT) {
        // 非精确压缩：组头仅对有效行生效，避免无效区干扰
        bs_bool = BoolAndBatchOperator(&bs_bool, &_dataCols[validIdx], 1, 0, msgTagBase,
                                       SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        // vs := valid ? vs : MAX_VALUE（避免无效行的值影响比较）
        std::vector<int64_t> max_vals(n, (1LL << (bitlen - 1)) - 1);
        vs = BoolMutexBatchOperator(&vs, &max_vals, &_dataCols[validIdx], bitlen, 0, msgTagBase,
                                    SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    }

    // 各算子 stride
    const int lessStride = BoolLessBatchOperator::tagStride();
    const int mutexStride = BoolMutexBatchOperator::tagStride();
    const int andStride = BoolAndBatchOperator::tagStride();
    // 每个批任务用到：Less(1) + Mutex(1) + Mutex(1) + AND(1)
    const int taskStride = lessStride + mutexStride + mutexStride + andStride;

    int tagCursorBase = msgTagBase;
    const int64_t NOT_mask = rank;

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

                // 快照左右值
                std::vector<int64_t> left_vals(len), right_vals(len);
                for (int i = 0; i < len; ++i) {
                    const int idxL = start + i;
                    const int idxR = idxL + delta;
                    left_vals[i] = vs[idxL];
                    right_vals[i] = vs[idxR];
                }

                // 非精确压缩：每轮窗口再按窗口内 valid 掩蔽一次
                if (APPROX_COMPACT) {
                    auto &valid = _dataCols[validIdx];
                    std::vector<int64_t> max_vals(len, (1LL << (bitlen - 1)) - 1), left_valid(len), right_valid(len);
                    for (int i = 0; i < len; ++i) {
                        left_valid[i] = valid[start + i];
                        right_valid[i] = valid[start + i + delta];
                    }
                    left_vals = BoolMutexBatchOperator(&left_vals, &max_vals, &left_valid, bitlen, 0, lessTag,
                                                       SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
                    right_vals = BoolMutexBatchOperator(&right_vals, &max_vals, &right_valid, bitlen, 0, lessTag,
                                                        SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
                }

                // 1) cmp = (left < right)
                auto cmp = BoolLessBatchOperator(&left_vals, &right_vals, bitlen,
                                                 /*tid*/0, /*tag*/lessTag,
                                                 SecureOperator::NO_CLIENT_COMPUTE)
                        .execute()->_zis;

                // 2) cand = min(left,right) : cand = cmp ? left : right
                auto cand = BoolMutexBatchOperator(&left_vals, &right_vals, &cmp,
                                                   bitlen, /*tid*/0, /*tag*/mtx1Tag,
                                                   SecureOperator::NO_CLIENT_COMPUTE)
                        .execute()->_zis;

                // 3) gate = ~b[i+delta]；new_right = gate ? cand : right
                std::vector<int64_t> not_bR(len);
                for (int i = 0; i < len; ++i) {
                    not_bR[i] = bs_bool[start + i + delta] ^ NOT_mask;
                }
                auto new_right = BoolMutexBatchOperator(&cand, &right_vals, &not_bR,
                                                        bitlen, /*tid*/0, /*tag*/mtx2Tag,
                                                        SecureOperator::NO_CLIENT_COMPUTE)
                        .execute()->_zis;

                // 4) 边界传播：b[i+delta] = b[i] OR b[i+delta] = ~(~b[i] & ~b[i+delta])
                std::vector<int64_t> not_bL(len);
                for (int i = 0; i < len; ++i) {
                    not_bL[i] = bs_bool[start + i] ^ NOT_mask;
                    not_bR[i] = bs_bool[start + i + delta] ^ NOT_mask;
                }
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

    // —— 构造组尾
    std::vector<int64_t> group_tails(n);
    for (size_t i = 0; i + 1 < n; ++i) group_tails[i] = heads[i + 1];

    // 非精确压缩：修正"最后一组尾巴落到无效区"的情况
    if (APPROX_COMPACT) {
        auto &valid = _dataCols[validIdx];
        std::vector<int64_t> next_valid(n);
        for (size_t i = 0; i + 1 < n; ++i) next_valid[i] = valid[i + 1];
        next_valid[n - 1] = 0; // 0_share

        // last_valid_tail = valid ∧ ¬next_valid
        std::vector<int64_t> not_next(n);
        for (size_t i = 0; i < n; ++i) not_next[i] = next_valid[i] ^ rank;

        auto last_valid_tail = BoolAndBatchOperator(&valid, &not_next, 1, 0, msgTagBase,
                                                    SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        // group_tails = group_tails OR last_valid_tail  （德摩根）
        std::vector<int64_t> not_gt(n), not_lvt(n);
        for (size_t i = 0; i < n; ++i) { 
            not_gt[i] = group_tails[i] ^ rank; 
            not_lvt[i] = last_valid_tail[i] ^ rank; 
        }
        auto and_not = BoolAndBatchOperator(&not_gt, &not_lvt, 1, 0, msgTagBase,
                                            SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        for (size_t i = 0; i < n; ++i) group_tails[i] = and_not[i] ^ rank;
    } else {
        group_tails[n - 1] = rank;
    }

    // 插入结果列（位置与单批版一致：在 VALID 之前）
    const std::string outName = alias.empty() ? ("min_" + fieldName) : alias;

    _fieldNames.reserve(_fieldNames.size() + 1);
    _fieldWidths.reserve(_fieldWidths.size() + 1);
    _dataCols.reserve(_dataCols.size() + 1);

    const int insertPos = colNum() - 2;
    _fieldNames.insert(_fieldNames.begin() + insertPos, outName);
    _fieldWidths.insert(_fieldWidths.begin() + insertPos, bitlen);
    _dataCols.insert(_dataCols.begin() + insertPos, std::move(vs));

    validIdx = colNum() + VALID_COL_OFFSET;
    // —— 根据模式更新 VALID 并压缩
    if (APPROX_COMPACT) {
        auto new_valid = BoolAndBatchOperator(&_dataCols[validIdx], &group_tails, 1, 0, msgTagBase,
                                              SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        _dataCols[validIdx] = std::move(new_valid);
        clearInvalidEntries(msgTagBase); // 近似截断
    } else {
        _dataCols[validIdx] = std::move(group_tails);
        clearInvalidEntries(msgTagBase); // 精确截断
    }
}

int View::sortTagStride() {
    return ((rowNum() / 2 + Conf::BATCH_SIZE - 1) / Conf::BATCH_SIZE) * (colNum() - 1) *
           BoolMutexBatchOperator::tagStride();
}

void View::filterSingleBatch(std::vector<std::string> &fieldNames,
                             std::vector<ComparatorType> &comparatorTypes,
                             std::vector<int64_t> &constShares, bool clear, int msgTagBase) {
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

    if (DbConf::BASELINE_MODE) {
        _dataCols[validColIndex] = BoolAndBatchOperator(&result, &_dataCols[validColIndex], 1, 0, msgTagBase,
                                                        SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    } else if (!DbConf::DISABLE_PRECISE_COMPACTION) {
        _dataCols[validColIndex] = std::move(result);
        if (clear) {
            clearInvalidEntries(msgTagBase);
        }
    } else {
        // approximate compaction
        _dataCols[validColIndex] = BoolAndBatchOperator(&result, &_dataCols[validColIndex], 1, 0, msgTagBase,
                                                        SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        if (clear) {
            clearInvalidEntries(msgTagBase);
        }
    }
}

void View::filterMultiBatches(std::vector<std::string> &fieldNames,
                              std::vector<ComparatorType> &comparatorTypes,
                              std::vector<int64_t> &constShares, bool clear,
                              int msgTagBase) {
    if (rowNum() <= Conf::BATCH_SIZE) {
        // 走单批实现（里面已处理精确/非精确压缩分支）
        filterSingleBatch(fieldNames, comparatorTypes, constShares, clear, msgTagBase);
        return;
    }

    const size_t n = fieldNames.size();
    std::vector<std::vector<int64_t> > collected(n);

    // 并行计算每个条件的布尔列
    std::vector<std::future<std::vector<int64_t> > > futures(n);
    const size_t data_size = _dataCols[0].size();
    const int batchSize = Conf::BATCH_SIZE;
    const int batchNum = static_cast<int>((data_size + batchSize - 1) / batchSize);
    const int tagOffset = std::max(BoolLessBatchOperator::tagStride(),
                                   BoolEqualBatchOperator::tagStride());

    for (int i = 0; i < static_cast<int>(n); ++i) {
        futures[i] = ThreadPoolSupport::submit([&, i] {
            int colIndex = 0;
            for (int j = 0; j < static_cast<int>(_fieldNames.size()); ++j) {
                if (_fieldNames[j] == fieldNames[i]) {
                    colIndex = j;
                    break;
                }
            }
            auto ct = comparatorTypes[i];

            std::vector<std::future<std::vector<int64_t> > > batch_futures(batchNum);
            const int startTag = tagOffset * batchNum * i;

            for (int b = 0; b < batchNum; ++b) {
                const int start = b * batchSize;
                const int end = std::min(start + batchSize, static_cast<int>(data_size));
                const int curSz = end - start;

                batch_futures[b] = ThreadPoolSupport::submit(
                    [&, start, end, ct, colIndex, startTag, b, curSz]() {
                        std::vector<int64_t> batch_const(curSz, constShares[i]);
                        std::vector<int64_t> batch_data(_dataCols[colIndex].begin() + start,
                                                        _dataCols[colIndex].begin() + end);

                        const int batch_start_tag = startTag + b * tagOffset;
                        std::vector<int64_t> batch_result;

                        switch (ct) {
                            case GREATER:
                            case LESS_EQ: {
                                batch_result = BoolLessBatchOperator(&batch_const, &batch_data,
                                                                     _fieldWidths[colIndex], 0, batch_start_tag,
                                                                     SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
                                if (ct == LESS_EQ) {
                                    for (auto &v: batch_result) v ^= Comm::rank();
                                }
                                break;
                            }
                            case LESS:
                            case GREATER_EQ: {
                                batch_result = BoolLessBatchOperator(&batch_data, &batch_const,
                                                                     _fieldWidths[colIndex], 0, batch_start_tag,
                                                                     SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
                                if (ct == GREATER_EQ) {
                                    for (auto &v: batch_result) v ^= Comm::rank();
                                }
                                break;
                            }
                            case EQUALS:
                            case NOT_EQUALS: {
                                batch_result = BoolEqualBatchOperator(&batch_const, &batch_data,
                                                                      _fieldWidths[colIndex], 0, batch_start_tag,
                                                                      SecureOperator::NO_CLIENT_COMPUTE).execute()->
                                        _zis;
                                if (ct == NOT_EQUALS) {
                                    for (auto &v: batch_result) v ^= Comm::rank();
                                }
                                break;
                            }
                        }
                        return batch_result;
                    }
                );
            }

            std::vector<int64_t> currentCondition;
            currentCondition.reserve(data_size);
            for (auto &f: batch_futures) {
                auto br = f.get();
                currentCondition.insert(currentCondition.end(), br.begin(), br.end());
            }
            return currentCondition;
        });
    }

    for (int i = 0; i < static_cast<int>(n); ++i) {
        collected[i] = futures[i].get();
    }

    const int validColIndex = colNum() + VALID_COL_OFFSET;

    // === 合并所有条件（AND 归约） ===
    // n==1 也做成与多条件同样的后续分支，便于做 compaction 策略
    std::vector<int64_t> result;
    if (n == 1) {
        result = std::move(collected[0]);
    } else {
        std::vector<std::vector<int64_t> > andCols = std::move(collected);
        int level = 0;
        while (andCols.size() > 1) {
            const int m = static_cast<int>(andCols.size());
            const int pairs = m / 2;
            const int msgStride = BoolAndBatchOperator::tagStride();

            std::vector<std::future<std::vector<int64_t> > > futures1;
            futures1.reserve(pairs);

            for (int p = 0; p < pairs; ++p) {
                futures1.emplace_back(
                    ThreadPoolSupport::submit([&, p, level]() {
                        auto &L = andCols[2 * p];
                        auto &R = andCols[2 * p + 1];
                        return BoolAndBatchOperator(&L, &R, 1, 0,
                                                    msgTagBase + level * msgStride + p,
                                                    SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
                    })
                );
            }

            std::vector<std::vector<int64_t> > nextCols;
            nextCols.reserve((m + 1) / 2);

            for (int p = 0; p < pairs; ++p) nextCols.push_back(futures1[p].get());
            if (m % 2) nextCols.push_back(std::move(andCols.back()));

            andCols.swap(nextCols);
            ++level;
        }
        // 归约完成
        result = std::move(andCols.front());
    }

    // === compaction 分支 ===
    if (!DbConf::DISABLE_PRECISE_COMPACTION) {
        // 精确压缩：直接覆盖 VALID，用 clearInvalidEntries() 精确裁剪
        _dataCols[validColIndex] = std::move(result);
        if (clear) {
            clearInvalidEntries(msgTagBase);
        }
    } else {
        // 非精确压缩：与已有 VALID 取 AND，再用你的“近似截断”
        _dataCols[validColIndex] =
                BoolAndBatchOperator(&_dataCols[validColIndex], &result,
                                     1, 0, msgTagBase,
                                     SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        if (clear) {
            clearInvalidEntries(msgTagBase);
        }
    }
}

void View::filterAndConditions(std::vector<std::string> &fieldNames, std::vector<ComparatorType> &comparatorTypes,
                               std::vector<int64_t> &constShares, int msgTagBase) {
    if (Conf::BATCH_SIZE <= 0 || Conf::DISABLE_MULTI_THREAD) {
        filterSingleBatch(fieldNames, comparatorTypes, constShares, true, msgTagBase);
    } else {
        filterMultiBatches(fieldNames, comparatorTypes, constShares, true, msgTagBase);
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

    if (DbConf::DISABLE_PRECISE_COMPACTION) {
        std::vector<int64_t> sv = {sumShare};
        auto sumBoolShare = ArithToBoolBatchOperator(&sv, 64, 0, msgTagBase,
                                                     SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis[0];
        std::vector<bool> bits(64);
        for (int i = 0; i < 64; i++) {
            bits[i] = Math::getBit(sumBoolShare, i);
        }
        int msb = -1;
        for (int b = 63; b >= 0; --b) {
            int64_t bit_i = bits[b];
            int64_t bit_o = 0;

            auto s = Comm::serverSendAsync(bit_i, /*bits*/1, msgTagBase);
            auto r = Comm::serverReceiveAsync(bit_o, /*bits*/1, msgTagBase);
            Comm::wait(s);
            Comm::wait(r);

            int bit = static_cast<int>(bit_i ^ bit_o); // XOR 布尔还原
            if (bit) {
                msb = b;
                break;
            }
        }

        if (msb == 63) {
            return; // all valid
        }

        // 3) 计算保留行数：keep = 2^msb（若 msb<0，则 keep=0）
        size_t n = rowNum();
        size_t keep = std::min(n, msb >= 0 ? static_cast<size_t>(1) << (msb + 1) : 0);

        // 4) 截断：仅保留前 keep 行（排序后前缀全是有效或近似足够）
        for (auto &col: _dataCols) {
            col.resize(keep);
        }
    } else {
        auto r0 = Comm::serverSendAsync(sumShare, 32, msgTagBase);
        auto r1 = Comm::serverReceiveAsync(sumShare1, 32, msgTagBase);
        Comm::wait(r0);
        Comm::wait(r1);

        int64_t validNum = sumShare + sumShare1;


        for (auto &v: _dataCols) {
            v.resize(validNum);
        }
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
    const bool NO_COMPACTION = DbConf::BASELINE_MODE;

    // 仅在“不压缩”模式下需要参与聚合的有效位
    std::vector<int64_t> valid_col;
    if (NO_COMPACTION) {
        valid_col = _dataCols[colNum() + VALID_COL_OFFSET]; // 布尔分享
    }

    // === 分段扫描（并行前缀） ===
    // tag 规划（避免冲突）
    int tag_off_less = BoolLessBatchOperator::tagStride();
    int tag_off_and = BoolAndBatchOperator::tagStride();
    int tag_off_mux = BoolMutexBatchOperator::tagStride();

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
                                              msgTagBase + round * (tag_off_less + tag_off_and + 2 * tag_off_mux) +
                                              tag_off_less,
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
                                                        msgTagBase + round * (
                                                            tag_off_less + tag_off_and + 2 * tag_off_mux) + 2 *
                                                        tag_off_less,
                                                        SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
            std::vector<int64_t> or_notvr_or_less(m);
            for (int i = 0; i < m; ++i) or_notvr_or_less[i] = vr_and_not_less[i] ^ NOT_mask;
            sel_min = BoolAndBatchOperator(&vl, &or_notvr_or_less, 1, 0,
                                           msgTagBase + round * (tag_off_less + tag_off_and + 2 * tag_off_mux) + 2 *
                                           tag_off_less + tag_off_and,
                                           SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

            // sel_max = (¬vl) ∨ (vl ∧ vr ∧ less_max)
            auto vl_and_vr = BoolAndBatchOperator(&vl, &vr, 1, 0,
                                                  msgTagBase + round * (tag_off_less + tag_off_and + 2 * tag_off_mux) +
                                                  2 * tag_off_less + 2 * tag_off_and,
                                                  SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
            auto and_all = BoolAndBatchOperator(&vl_and_vr, &less_max, 1, 0,
                                                msgTagBase + round * (tag_off_less + tag_off_and + 2 * tag_off_mux) + 2
                                                * tag_off_less + 3 * tag_off_and,
                                                SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
            // OR via DeMorgan: (¬vl) ∨ and_all = ¬( vl ∧ ¬and_all )
            std::vector<int64_t> not_and_all(m);
            for (int i = 0; i < m; ++i) not_and_all[i] = and_all[i] ^ NOT_mask;
            auto vl_and_not = BoolAndBatchOperator(&vl, &not_and_all, 1, 0,
                                                   msgTagBase + round * (tag_off_less + tag_off_and + 2 * tag_off_mux) +
                                                   2 * tag_off_less + 4 * tag_off_and,
                                                   SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
            sel_max.resize(m);
            for (int i = 0; i < m; ++i) sel_max[i] = vl_and_not[i] ^ NOT_mask;
        }

        // 候选：min / max
        auto min_cand = BoolMutexBatchOperator(&min_L, &min_R, &sel_min, bitlen, 0,
                                               msgTagBase + round * (tag_off_less + tag_off_and + 2 * tag_off_mux) + 2 *
                                               tag_off_less + 5 * tag_off_and,
                                               SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        auto max_cand = BoolMutexBatchOperator(&max_R, &max_L, &sel_max, bitlen, 0,
                                               msgTagBase + round * (tag_off_less + tag_off_and + 2 * tag_off_mux) + 2 *
                                               tag_off_less + 5 * tag_off_and + tag_off_mux,
                                               SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        // gate = ¬b[i+δ]（仍按原分段逻辑）
        std::vector<int64_t> gate(m);
        for (int i = 0; i < m; ++i) gate[i] = (bs_bool[i + delta] ^ NOT_mask);

        // new_right = gate ? cand : right
        auto min_new_right = BoolMutexBatchOperator(&min_cand, &min_R, &gate, bitlen, 0,
                                                    msgTagBase + round * (tag_off_less + tag_off_and + 2 * tag_off_mux)
                                                    + 2 * tag_off_less + 5 * tag_off_and + 2 * tag_off_mux,
                                                    SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        auto max_new_right = BoolMutexBatchOperator(&max_cand, &max_R, &gate, bitlen, 0,
                                                    msgTagBase + round * (tag_off_less + tag_off_and + 2 * tag_off_mux)
                                                    + 2 * tag_off_less + 5 * tag_off_and + 3 * tag_off_mux,
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
                                            msgTagBase + round * (tag_off_less + tag_off_and + 2 * tag_off_mux) + 2 *
                                            tag_off_less + 5 * tag_off_and + 4 * tag_off_mux,
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

    if (!DbConf::BASELINE_MODE && !DbConf::DISABLE_PRECISE_COMPACTION) {
        // First, sort by the group field to ensure records with same keys are adjacent
        sort(groupField, true, msgTagBase);
    } else {
        std::vector<std::string> sortFields;
        std::vector<bool> ascending;
        sortFields.push_back(VALID_COL_NAME);
        ascending.push_back(false);
        sortFields.push_back(groupField);
        ascending.push_back(true);
        sort(sortFields, ascending, msgTagBase);
    }

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

    if (!DbConf::BASELINE_MODE && !DbConf::DISABLE_PRECISE_COMPACTION) {
        std::vector<bool> ascendingOrders(groupFields.size(), true);
        sort(groupFields, ascendingOrders, msgTagBase);
    } else {
        std::vector<std::string> sortFields;
        std::vector<bool> ascending;
        sortFields.push_back(VALID_COL_NAME);
        ascending.push_back(false);
        for (const auto &gf: groupFields) {
            sortFields.push_back(gf);
            ascending.push_back(true); // 组键升序
        }
        sort(sortFields, ascending, msgTagBase);
    }
    // Step 1: Sort by all group fields to ensure records with same keys are adjacent

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

void View::distinct(int msgTagBase) {
    size_t rn = rowNum();
    if (rn == 0) {
        return;
    }
    size_t cn = colNum();

    std::vector<std::string> fields;
    for (int i = 0; i < cn - 2; i++) {
        if (StringUtils::hasPrefix(_fieldNames[i], BUCKET_TAG_PREFIX)) {
            continue;
        }
        fields.push_back(_fieldNames[i]);
    }

    auto heads = groupBy(fields, msgTagBase);
    _dataCols[cn + VALID_COL_OFFSET] = std::move(heads);
    clearInvalidEntries(msgTagBase);
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

void View::filterAndConditions(std::vector<std::string> &fieldNames, std::vector<ComparatorType> &comparatorTypes,
    std::vector<int64_t> &constShares, bool clear, int msgTagBase) {
    if (Conf::BATCH_SIZE <= 0 || Conf::DISABLE_MULTI_THREAD) {
        filterSingleBatch(fieldNames, comparatorTypes, constShares, clear, msgTagBase);
    } else {
        filterMultiBatches(fieldNames, comparatorTypes, constShares, clear, msgTagBase);
    }
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
