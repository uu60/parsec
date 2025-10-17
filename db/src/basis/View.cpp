
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

    int totalCols = static_cast<int>(colNum());
    int validColIndex = totalCols + VALID_COL_OFFSET;
    int paddingColIndex = totalCols + PADDING_COL_OFFSET;

    std::vector<std::vector<int64_t> > newDataCols;
    std::vector<std::string> newFieldNames;
    std::vector<int> newFieldWidths;

    size_t newSize = selectedIndices.size() + 2;
    newDataCols.reserve(newSize);
    newFieldNames.reserve(newSize);
    newFieldWidths.reserve(newSize);

    for (int index: selectedIndices) {
        newDataCols.push_back(std::move(_dataCols[index]));
        newFieldNames.push_back(std::move(_fieldNames[index]));
        newFieldWidths.push_back(_fieldWidths[index]);
    }

    newDataCols.push_back(std::move(_dataCols[validColIndex]));
    newDataCols.push_back(std::move(_dataCols[paddingColIndex]));
    newFieldNames.push_back(VALID_COL_NAME);
    newFieldNames.push_back(PADDING_COL_NAME);
    newFieldWidths.push_back(1);
    newFieldWidths.push_back(1);

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

    std::vector<int64_t> eqs;
    if (n > 1) {
        std::vector<int64_t> curr, prev;
        curr.reserve(n - 1);
        prev.reserve(n - 1);
        for (size_t i = 1; i < n; ++i) {
            curr.push_back(keys[i]);
            prev.push_back(keys[i - 1]);
        }
        eqs = BoolEqualBatchOperator(&curr, &prev, bw, 0, msgTagBase,
                                     SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    }

    const bool precise = (!DbConf::BASELINE_MODE) && (!DbConf::DISABLE_PRECISE_COMPACTION);

    std::vector<int64_t> heads(n, 0);

    if (precise) {
        heads[0] = Comm::rank();
        if (n > 1) {
            for (size_t i = 1; i < n; ++i) {
                heads[i] = eqs[i - 1] ^ Comm::rank();
            }
        }
        return heads;
    }

    const int validIdx = colNum() + VALID_COL_OFFSET;
    auto &validCol = _dataCols[validIdx];

    heads[0] = (n > 0 ? validCol[0] : 0);

    if (n > 1) {
        std::vector<int64_t> neq(n - 1);
        for (size_t i = 0; i < n - 1; ++i) neq[i] = eqs[i] ^ Comm::rank();

        std::vector<int64_t> valid_tail(n - 1);
        for (size_t i = 1; i < n; ++i) valid_tail[i - 1] = validCol[i];

        auto heads_tail = BoolAndBatchOperator(&valid_tail, &neq, 1,
                                               0, msgTagBase,
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
                const int start = b * batchSize + 1;
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
                return BoolEqualBatchOperator(&curr, &prev, bw, 0, batchTag,
                                              SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
            });
        }
        for (auto &f: batchFuts) {
            auto part = f.get();
            eqs.insert(eqs.end(), part.begin(), part.end());
        }
    }

    const bool precise = !DbConf::DISABLE_PRECISE_COMPACTION;
    std::vector<int64_t> heads(n, 0);

    if (precise) {
        heads[0] = Comm::rank();
        if (n > 1) {
            for (size_t i = 1; i < n; ++i) {
                heads[i] = eqs[i - 1] ^ Comm::rank();
            }
        }
        return heads;
    }

    const int validIdx = colNum() + VALID_COL_OFFSET;
    auto &validCol = _dataCols[validIdx];

    heads[0] = validCol[0];

    if (n > 1) {
        std::vector<int64_t> neq(n - 1);
        for (size_t i = 0; i < n - 1; ++i) neq[i] = eqs[i] ^ Comm::rank();

        std::vector<int64_t> valid_tail(n - 1);
        for (size_t i = 1; i < n; ++i) valid_tail[i - 1] = validCol[i];

        auto heads_tail = BoolAndBatchOperator(&valid_tail, &neq, 1,
                                               0, msgTagBase,
                                               SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        for (size_t i = 1; i < n; ++i) heads[i] = heads_tail[i - 1];
    }

    return heads;
}

std::vector<int64_t> View::groupBySingleBatch(const std::vector<std::string> &groupFields, int msgTagBase) {
    const size_t n = rowNum();
    std::vector<int64_t> groupHeads(n, 0);
    if (n == 0 || groupFields.empty()) return groupHeads;

    std::vector<int> idx(groupFields.size());
    for (size_t k = 0; k < groupFields.size(); ++k) {
        idx[k] = colIndex(groupFields[k]);
        if (idx[k] < 0) {
            return groupHeads;
        }
    }

    std::vector<int64_t> eq_all;
    if (n > 1) {
        {
            std::vector<int64_t> cur(n - 1), prv(n - 1);
            for (size_t i = 1; i < n; ++i) {
                cur[i - 1] = _dataCols[idx[0]][i];
                prv[i - 1] = _dataCols[idx[0]][i - 1];
            }
            eq_all = BoolEqualBatchOperator(&cur, &prv, _fieldWidths[idx[0]],
                                            0, msgTagBase,
                                            SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        }
        for (size_t k = 1; k < idx.size(); ++k) {
            std::vector<int64_t> cur(n - 1), prv(n - 1);
            for (size_t i = 1; i < n; ++i) {
                cur[i - 1] = _dataCols[idx[k]][i];
                prv[i - 1] = _dataCols[idx[k]][i - 1];
            }
            auto eq_k = BoolEqualBatchOperator(&cur, &prv, _fieldWidths[idx[k]],
                                               0, msgTagBase,
                                               SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
            eq_all = BoolAndBatchOperator(&eq_all, &eq_k, 1,
                                          0, msgTagBase,
                                          SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        }
    }

    const bool precise = !DbConf::DISABLE_PRECISE_COMPACTION;
    const int validColIndex = colNum() + VALID_COL_OFFSET;
    const auto &validCol = _dataCols[validColIndex];
    const int64_t NOT_mask = Comm::rank();

    if (precise) {
        groupHeads[0] = Comm::rank();
        for (size_t i = 1; i < n; ++i) {
            groupHeads[i] = (i - 1 < eq_all.size()) ? (eq_all[i - 1] ^ NOT_mask) : Comm::rank();
        }
    } else {
        groupHeads[0] = validCol[0];
        if (n > 1) {
            std::vector<int64_t> neq(n - 1);
            for (size_t i = 1; i < n; ++i) {
                neq[i - 1] = eq_all[i - 1] ^ NOT_mask;
            }
            std::vector<int64_t> valid_tail(n - 1);
            for (size_t i = 1; i < n; ++i) valid_tail[i - 1] = validCol[i];

            auto and_res = BoolAndBatchOperator(&valid_tail, &neq, 1,
                                                0, msgTagBase,
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

    std::vector<int> gIdx(groupFields.size());
    for (size_t k = 0; k < groupFields.size(); ++k) {
        gIdx[k] = colIndex(groupFields[k]);
    }

    std::vector<int64_t> groupHeads(n, 0);
    std::vector<int64_t> eq_all;
    eq_all.reserve(n > 0 ? n - 1 : 0);

    if (n > 1) {
        const int batchSize = Conf::BATCH_SIZE;
        const int pairCnt = static_cast<int>(n - 1);
        const int batchNum = (pairCnt + batchSize - 1) / batchSize;
        const int tagOffset = std::max(BoolEqualBatchOperator::tagStride(), BoolAndBatchOperator::tagStride());

        std::vector<std::future<std::vector<int64_t> > > colFuts(groupFields.size());
        for (int colIdx = 0; colIdx < static_cast<int>(groupFields.size()); ++colIdx) {
            colFuts[colIdx] = ThreadPoolSupport::submit([&, colIdx] {
                const int fidx = gIdx[colIdx];
                std::vector<int64_t> colEq;
                colEq.reserve(n - 1);

                std::vector<std::future<std::vector<int64_t> > > batchFuts(batchNum);
                const int startTag = tagOffset * batchNum * colIdx;

                for (int b = 0; b < batchNum; ++b) {
                    batchFuts[b] = ThreadPoolSupport::submit([&, b, fidx, startTag]() {
                        const int start = b * batchSize + 1;
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
                            0, batchTag,
                            SecureOperator::NO_CLIENT_COMPUTE
                        ).execute()->_zis;
                    });
                }

                for (auto &bf: batchFuts) {
                    auto part = bf.get();
                    colEq.insert(colEq.end(), part.begin(), part.end());
                }
                return colEq;
            });
        }

        std::vector<std::vector<int64_t> > allCols(groupFields.size());
        for (int colIdx = 0; colIdx < static_cast<int>(groupFields.size()); ++colIdx) {
            allCols[colIdx] = colFuts[colIdx].get();
        }

        if (!allCols.empty()) {
            eq_all = std::move(allCols[0]);
            for (int colIdx = 1; colIdx < static_cast<int>(allCols.size()); ++colIdx) {
                eq_all = BoolAndBatchOperator(
                    &eq_all, &allCols[colIdx], 1,
                    0, msgTagBase,
                    SecureOperator::NO_CLIENT_COMPUTE
                ).execute()->_zis;
            }
        }
    }

    const int64_t NOT_mask = Comm::rank();

    if (!DbConf::DISABLE_PRECISE_COMPACTION) {
        groupHeads[0] = Comm::rank();
        for (size_t i = 1; i < n; ++i) {
            groupHeads[i] = (i - 1 < eq_all.size()) ? (eq_all[i - 1] ^ NOT_mask) : Comm::rank();
        }
    } else {
        const int validColIndex = colNum() + VALID_COL_OFFSET;
        auto &validCol = _dataCols[validColIndex];

        groupHeads[0] = (n > 0 ? validCol[0] : 0);

        if (n > 1) {
            std::vector<int64_t> neq(eq_all.size());
            for (size_t j = 0; j < eq_all.size(); ++j) neq[j] = eq_all[j] ^ NOT_mask;

            std::vector<int64_t> valid_tail(eq_all.size());
            for (size_t j = 0; j < eq_all.size(); ++j) valid_tail[j] = validCol[j + 1];

            auto tail_heads = BoolAndBatchOperator(
                &valid_tail, &neq, 1,
                0, msgTagBase,
                SecureOperator::NO_CLIENT_COMPUTE
            ).execute()->_zis;

            for (size_t i = 1; i < n; ++i) {
                groupHeads[i] = tail_heads[i - 1];
            }
        }
    }

    return groupHeads;
}

void View::countSingleBatch(std::vector<int64_t> &heads, std::string alias, int msgTagBase, std::string matchedTable,
                            bool compress) {
    const size_t n = rowNum();
    if (n == 0) return;

    const bool BASELINE = DbConf::BASELINE_MODE;
    const bool APPROX_COMPACT = DbConf::DISABLE_PRECISE_COMPACTION;
    const int validIdx = colNum() + VALID_COL_OFFSET;
    const int64_t rank = Comm::rank();

    std::vector<int64_t> bs_bool = heads;

    if (BASELINE || APPROX_COMPACT) {
        bs_bool = BoolAndBatchOperator(&bs_bool, &_dataCols[validIdx], 1, 0, msgTagBase,
                                       SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    }

    std::vector<int64_t> counts(n, 0);
    if (BASELINE || APPROX_COMPACT) {
        counts = _dataCols[validIdx];
        if (!matchedTable.empty()) {
            counts = BoolAndBatchOperator(&counts, &_dataCols[colIndex(OUTER_MATCH_PREFIX + matchedTable)], 1, 0,
                                          msgTagBase, SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        }
    } else {
        for (size_t i = 0; i < n; ++i) {
            counts[i] = rank;
        }
    }

    auto counts_arith = BoolToArithBatchOperator(&counts, 64, 0, msgTagBase,
                                                 SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

    auto bs_arith = BoolToArithBatchOperator(&bs_bool, 64, 0, msgTagBase,
                                             SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

    for (int delta = 1; delta < static_cast<int>(n); delta <<= 1) {
        const int m = static_cast<int>(n) - delta;
        if (m <= 0) break;

        std::vector<int64_t> propagate_mask(m);
        for (int i = 0; i < m; ++i) {
            propagate_mask[i] = rank - bs_arith[i + delta];
        }

        std::vector<int64_t> add_values(m);
        for (int i = 0; i < m; ++i) {
            add_values[i] = counts_arith[i];
        }

        auto increments = ArithMultiplyBatchOperator(&propagate_mask, &add_values, 64, 0, msgTagBase,
                                                     SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        for (int i = 0; i < m; ++i) {
            counts_arith[i + delta] += increments[i];
        }

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

        bs_arith = BoolToArithBatchOperator(&bs_bool, 64, 0, msgTagBase,
                                            SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    }

    auto counts_bool = ArithToBoolBatchOperator(&counts_arith, 64, 0, msgTagBase,
                                                SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

    std::vector<int64_t> group_tails(n);
    for (size_t i = 0; i + 1 < n; ++i) {
        group_tails[i] = heads[i + 1];
    }

    if (BASELINE || APPROX_COMPACT) {
        auto &valid = _dataCols[validIdx];
        std::vector<int64_t> next_valid(n);
        for (size_t i = 0; i + 1 < n; ++i) {
            next_valid[i] = valid[i + 1];
        }
        next_valid[n - 1] = 0;

        std::vector<int64_t> not_next(n);
        for (size_t i = 0; i < n; ++i) {
            not_next[i] = next_valid[i] ^ rank;
        }
        auto boundary = BoolAndBatchOperator(&valid, &not_next, 1, 0, msgTagBase,
                                             SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

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
        group_tails[n - 1] = rank;
    }

    const int insertPos = colNum() - 2;
    _fieldNames.insert(_fieldNames.begin() + insertPos, alias.empty() ? COUNT_COL_NAME : alias);
    _fieldWidths.insert(_fieldWidths.begin() + insertPos, 64);
    _dataCols.insert(_dataCols.begin() + insertPos, std::move(counts_bool));

    const int newValidIdx = colNum() + VALID_COL_OFFSET;
    if (BASELINE) {
        auto new_valid = BoolAndBatchOperator(&_dataCols[newValidIdx], &group_tails, 1, 0, msgTagBase,
                                              SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        _dataCols[newValidIdx] = std::move(new_valid);
    } else if (APPROX_COMPACT) {
        auto new_valid = BoolAndBatchOperator(&_dataCols[newValidIdx], &group_tails, 1, 0, msgTagBase,
                                              SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        _dataCols[newValidIdx] = std::move(new_valid);
        if (compress) {
            clearInvalidEntries(msgTagBase);
        }
    } else {
        _dataCols[newValidIdx] = std::move(group_tails);
        if (compress) {
            clearInvalidEntries(msgTagBase);
        }
    }
}

void View::countMultiBatches(std::vector<int64_t> &heads, std::string alias, int msgTagBase, std::string matchedTable,
                             bool compress) {
    size_t n = rowNum();
    const int batchSize = Conf::BATCH_SIZE;
    if (n == 0) return;
    if (n <= static_cast<size_t>(batchSize)) {
        countSingleBatch(heads, alias, msgTagBase, matchedTable, compress);
        return;
    }

    const bool BASELINE = DbConf::BASELINE_MODE;
    const bool APPROX_COMPACT = DbConf::DISABLE_PRECISE_COMPACTION;
    const int validIdx = colNum() + VALID_COL_OFFSET;
    const int64_t rank = Comm::rank();

    std::vector<int64_t> bs_bool = heads;

    if (BASELINE || APPROX_COMPACT) {
        bs_bool = BoolAndBatchOperator(&bs_bool, &_dataCols[validIdx], 1, 0, msgTagBase,
                                       SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    }

    std::vector<int64_t> counts(n, 0);
    if (BASELINE || APPROX_COMPACT) {
        for (size_t i = 0; i < n; ++i) {
            counts[i] = _dataCols[validIdx][i];
        }
    } else {
        for (size_t i = 0; i < n; ++i) {
            counts[i] = rank;
        }
    }

    auto counts_arith = BoolToArithBatchOperator(&counts, 64, 0, msgTagBase,
                                                 SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

    auto bs_arith = BoolToArithBatchOperator(&bs_bool, 64, 0, msgTagBase,
                                             SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

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

        using Triple = std::tuple<
            std::vector<int64_t>,
            std::vector<int64_t>,
            std::vector<int64_t>
        >;

        std::vector<std::future<Triple> > futs(numBatches);

        for (int b = 0; b < numBatches; ++b) {
            const int start = b * batchSize;
            const int end = std::min(start + batchSize, totalPairs);
            const int len = end - start;

            const int baseTag = tagCursorBase + b * taskStride;
            const int mulTag = baseTag;
            const int andTag = baseTag + mulStride;
            const int b2aTag = baseTag + mulStride + andStride;

            futs[b] = ThreadPoolSupport::submit([=, &counts_arith, &bs_bool, &bs_arith]() -> Triple {
                if (len <= 0) return Triple{{}, {}, {}};

                std::vector<int64_t> propagate_mask(len);
                for (int i = 0; i < len; ++i) {
                    const int idx = start + i;
                    propagate_mask[i] = rank - bs_arith[idx + delta];
                }

                std::vector<int64_t> add_values(len);
                for (int i = 0; i < len; ++i) {
                    const int idx = start + i;
                    add_values[i] = counts_arith[idx];
                }

                auto increments = ArithMultiplyBatchOperator(&propagate_mask, &add_values, 64, 0, mulTag,
                                                             SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

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
                    new_bs_bool[i] = and_res[i] ^ rank;
                }

                auto new_bs_arith = BoolToArithBatchOperator(&new_bs_bool, 64, 0, b2aTag,
                                                             SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

                return Triple{std::move(increments), std::move(new_bs_bool), std::move(new_bs_arith)};
            });
        }

        tagCursorBase += numBatches * taskStride;

        for (int b = 0; b < numBatches; ++b) {
            const int start = b * batchSize;
            const int end = std::min(start + batchSize, totalPairs);
            const int len = end - start;
            if (len <= 0) continue;

            auto triple = futs[b].get();
            auto &increments = std::get<0>(triple);
            auto &new_b_bool = std::get<1>(triple);
            auto &new_b_arith = std::get<2>(triple);

            for (int i = 0; i < len; ++i) {
                counts_arith[start + i + delta] += increments[i];
                bs_bool[start + i + delta] = new_b_bool[i];
                bs_arith[start + i + delta] = new_b_arith[i];
            }
        }
    }

    std::vector<int64_t> counts_bool; {
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
            auto slice = std::make_shared<std::vector<int64_t> >(counts_arith.begin() + start,
                                                                 counts_arith.begin() + end);
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

    std::vector<int64_t> group_tails(n);
    for (size_t i = 0; i + 1 < n; ++i) {
        group_tails[i] = heads[i + 1];
    }

    if (BASELINE || APPROX_COMPACT) {
        auto &valid = _dataCols[validIdx];
        std::vector<int64_t> next_valid(n);
        for (size_t i = 0; i + 1 < n; ++i) {
            next_valid[i] = valid[i + 1];
        }
        next_valid[n - 1] = 0;

        std::vector<int64_t> not_next(n);
        for (size_t i = 0; i < n; ++i) {
            not_next[i] = next_valid[i] ^ rank;
        }
        auto boundary = BoolAndBatchOperator(&valid, &not_next, 1, 0, msgTagBase,
                                             SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

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
        group_tails[n - 1] = rank;
    }

    const int insertPos = colNum() - 2;
    _fieldNames.insert(_fieldNames.begin() + insertPos, alias.empty() ? COUNT_COL_NAME : alias);
    _fieldWidths.insert(_fieldWidths.begin() + insertPos, 64);
    _dataCols.insert(_dataCols.begin() + insertPos, std::move(counts_bool));

    const int newValidIdx = colNum() + VALID_COL_OFFSET;
    if (BASELINE) {
        auto new_valid = BoolAndBatchOperator(&_dataCols[newValidIdx], &group_tails, 1, 0, msgTagBase,
                                              SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        _dataCols[newValidIdx] = std::move(new_valid);
    } else if (APPROX_COMPACT) {
        auto new_valid = BoolAndBatchOperator(&_dataCols[newValidIdx], &group_tails, 1, 0, msgTagBase,
                                              SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        _dataCols[newValidIdx] = std::move(new_valid);
        if (compress) {
            clearInvalidEntries(msgTagBase);
        }
    } else {
        _dataCols[newValidIdx] = std::move(group_tails);
        if (compress) {
            clearInvalidEntries(msgTagBase);
        }
    }
}

void View::maxSingleBatch(std::vector<int64_t> &heads,
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

    const bool BASELINE = DbConf::BASELINE_MODE;
    const bool APPROX_COMPACT = DbConf::DISABLE_PRECISE_COMPACTION;
    const bool PRECISE_COMPACT = !BASELINE && !APPROX_COMPACT;

    std::vector<int64_t> &src = _dataCols[fieldIdx];
    if (src.size() != n || heads.size() != n) {
        Log::e("Size mismatch: fieldData={}, heads={}, n={}", src.size(), heads.size(), n);
        return;
    }

    std::vector<int64_t> vs = src;
    std::vector<int64_t> bs_bool = heads;
    const int64_t XOR_MASK = Comm::rank();

    int validIdx = colNum() + VALID_COL_OFFSET;
    if (BASELINE || APPROX_COMPACT) {
        auto &valid = _dataCols[validIdx];

        bs_bool = BoolAndBatchOperator(&bs_bool, &valid, 1, 0, msgTagBase,
                                       SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        std::vector<int64_t> zeros(n, 0);
        vs = BoolMutexBatchOperator(&vs, &zeros, &valid, bitlen, 0, msgTagBase,
                                    SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    }

    for (int delta = 1; delta < static_cast<int>(n); delta <<= 1) {
        const int m = static_cast<int>(n) - delta;

        std::vector<int64_t> left_vals(m), right_vals(m);
        for (int i = 0; i < m; ++i) {
            left_vals[i] = vs[i];
            right_vals[i] = vs[i + delta];
        }

        if (BASELINE || APPROX_COMPACT) {
            auto &valid = _dataCols[validIdx];
            std::vector<int64_t> zeros(m, 0), left_valid(m), right_valid(m);
            for (int i = 0; i < m; ++i) {
                left_valid[i] = valid[i];
                right_valid[i] = valid[i + delta];
            }
            left_vals = BoolMutexBatchOperator(&left_vals, &zeros, &left_valid, bitlen, 0, msgTagBase,
                                               SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
            right_vals = BoolMutexBatchOperator(&right_vals, &zeros, &right_valid, bitlen, 0, msgTagBase,
                                                SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        }

        auto cmp = BoolLessBatchOperator(&left_vals, &right_vals, bitlen, 0, msgTagBase,
                                         SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        auto cand = BoolMutexBatchOperator(&right_vals, &left_vals, &cmp, bitlen, 0, msgTagBase,
                                           SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        std::vector<int64_t> gate(m);
        for (int i = 0; i < m; ++i) gate[i] = (bs_bool[i + delta] ^ XOR_MASK);
        auto new_right = BoolMutexBatchOperator(&cand, &right_vals, &gate, bitlen, 0, msgTagBase,
                                                SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        for (int i = 0; i < m; ++i) vs[i + delta] = new_right[i];

        std::vector<int64_t> not_l(m), not_r(m);
        for (int i = 0; i < m; ++i) {
            not_l[i] = bs_bool[i] ^ XOR_MASK;
            not_r[i] = bs_bool[i + delta] ^ XOR_MASK;
        }
        auto and_out = BoolAndBatchOperator(&not_l, &not_r, 1, 0, msgTagBase,
                                            SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        for (int i = 0; i < m; ++i) bs_bool[i + delta] = (and_out[i] ^ XOR_MASK);
    }

    std::vector<int64_t> group_tails(n);
    for (size_t i = 0; i + 1 < n; ++i) group_tails[i] = heads[i + 1];

    if (PRECISE_COMPACT) {
        group_tails[n - 1] = Comm::rank();
    } else {
        auto &valid = _dataCols[validIdx];
        std::vector<int64_t> next_valid(n);
        for (size_t i = 0; i + 1 < n; ++i) next_valid[i] = valid[i + 1];
        next_valid[n - 1] = 0;

        std::vector<int64_t> not_next(n);
        for (size_t i = 0; i < n; ++i) not_next[i] = next_valid[i] ^ XOR_MASK;
        auto last_valid_tail = BoolAndBatchOperator(&valid, &not_next, 1, 0, msgTagBase,
                                                    SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        std::vector<int64_t> not_gt(n), not_lvt(n);
        for (size_t i = 0; i < n; ++i) {
            not_gt[i] = group_tails[i] ^ XOR_MASK;
            not_lvt[i] = last_valid_tail[i] ^ XOR_MASK;
        }
        auto and_not = BoolAndBatchOperator(&not_gt, &not_lvt, 1, 0, msgTagBase,
                                            SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        for (size_t i = 0; i < n; ++i) group_tails[i] = (and_not[i] ^ XOR_MASK);
    }

    const std::string outName = alias.empty() ? ("max_" + fieldName) : alias;
    _fieldNames.reserve(_fieldNames.size() + 1);
    _fieldWidths.reserve(_fieldWidths.size() + 1);
    _dataCols.reserve(_dataCols.size() + 1);

    const int insertPos = colNum() - 2;
    _fieldNames.insert(_fieldNames.begin() + insertPos, outName);
    _fieldWidths.insert(_fieldWidths.begin() + insertPos, bitlen);
    _dataCols.insert(_dataCols.begin() + insertPos, vs);

    validIdx = colNum() + VALID_COL_OFFSET;
    if (BASELINE) {
        auto new_valid = BoolAndBatchOperator(&_dataCols[validIdx], &group_tails, 1, 0, msgTagBase,
                                              SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        _dataCols[validIdx] = std::move(new_valid);
    } else if (APPROX_COMPACT) {
        auto new_valid = BoolAndBatchOperator(&_dataCols[validIdx], &group_tails, 1, 0, msgTagBase,
                                              SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        _dataCols[validIdx] = std::move(new_valid);
        clearInvalidEntries(msgTagBase);
    } else {
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
        maxSingleBatch(heads, fieldName, alias, msgTagBase);
        return;
    }

    const bool APPROX_COMPACT = DbConf::DISABLE_PRECISE_COMPACTION;
    const int64_t rank = Comm::rank();
    const int validIdx = colNum() + VALID_COL_OFFSET;

    std::vector<int64_t> bs_bool = heads;
    std::vector<int64_t> vs = src;

    if (APPROX_COMPACT) {
        bs_bool = BoolAndBatchOperator(&bs_bool, &_dataCols[validIdx], 1, 0, msgTagBase,
                                       SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        std::vector<int64_t> zeros(n, 0);
        vs = BoolMutexBatchOperator(&vs, &zeros, &_dataCols[validIdx], bitlen, 0, msgTagBase,
                                    SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    }

    const int lessStride = BoolLessBatchOperator::tagStride();
    const int mutexStride = BoolMutexBatchOperator::tagStride();
    const int andStride = BoolAndBatchOperator::tagStride();
    const int taskStride = lessStride + mutexStride + mutexStride + andStride;

    int tagCursorBase = msgTagBase;
    const int64_t NOT_mask = rank;

    for (int delta = 1; delta < static_cast<int>(n); delta <<= 1) {
        const int totalPairs = static_cast<int>(n) - delta;
        if (totalPairs <= 0) break;

        const int numBatches = (totalPairs + batchSize - 1) / batchSize;

        using Pair = std::pair<std::vector<int64_t>, std::vector<int64_t> >;

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
                for (int i = 0; i < len; ++i) {
                    const int idxL = start + i;
                    const int idxR = idxL + delta;
                    left_vals[i] = vs[idxL];
                    right_vals[i] = vs[idxR];
                }

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

                auto cmp = BoolLessBatchOperator(&left_vals, &right_vals, bitlen,
                                                 0, lessTag,
                                                 SecureOperator::NO_CLIENT_COMPUTE)
                        .execute()->_zis;

                auto cand = BoolMutexBatchOperator(&right_vals, &left_vals, &cmp,
                                                   bitlen, 0, mtx1Tag,
                                                   SecureOperator::NO_CLIENT_COMPUTE)
                        .execute()->_zis;

                std::vector<int64_t> not_bR(len);
                for (int i = 0; i < len; ++i) {
                    not_bR[i] = bs_bool[start + i + delta] ^ NOT_mask;
                }
                auto new_right = BoolMutexBatchOperator(&cand, &right_vals, &not_bR,
                                                        bitlen, 0, mtx2Tag,
                                                        SecureOperator::NO_CLIENT_COMPUTE)
                        .execute()->_zis;

                std::vector<int64_t> not_bL(len);
                for (int i = 0; i < len; ++i) {
                    not_bL[i] = bs_bool[start + i] ^ NOT_mask;
                    not_bR[i] = bs_bool[start + i + delta] ^ NOT_mask;
                }
                auto and_out = BoolAndBatchOperator(&not_bL, &not_bR,
                                                    1, 0, andTag,
                                                    SecureOperator::NO_CLIENT_COMPUTE)
                        .execute()->_zis;
                std::vector<int64_t> new_b(len);
                for (int i = 0; i < len; ++i) new_b[i] = and_out[i] ^ NOT_mask;

                return Pair{std::move(new_right), std::move(new_b)};
            });
        }

        tagCursorBase += numBatches * taskStride;

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

    std::vector<int64_t> group_tails(n);
    for (size_t i = 0; i + 1 < n; ++i) group_tails[i] = heads[i + 1];

    if (APPROX_COMPACT) {
        auto &valid = _dataCols[validIdx];
        std::vector<int64_t> next_valid(n);
        for (size_t i = 0; i + 1 < n; ++i) next_valid[i] = valid[i + 1];
        next_valid[n - 1] = 0;

        std::vector<int64_t> not_next(n);
        for (size_t i = 0; i < n; ++i) not_next[i] = next_valid[i] ^ rank;

        auto last_valid_tail = BoolAndBatchOperator(&valid, &not_next, 1, 0, msgTagBase,
                                                    SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

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

    const std::string outName = alias.empty() ? ("max_" + fieldName) : alias;

    _fieldNames.reserve(_fieldNames.size() + 1);
    _fieldWidths.reserve(_fieldWidths.size() + 1);
    _dataCols.reserve(_dataCols.size() + 1);

    const int insertPos = colNum() - 2;
    _fieldNames.insert(_fieldNames.begin() + insertPos, outName);
    _fieldWidths.insert(_fieldWidths.begin() + insertPos, bitlen);
    _dataCols.insert(_dataCols.begin() + insertPos, std::move(vs));

    if (APPROX_COMPACT) {
        auto new_valid = BoolAndBatchOperator(&_dataCols[validIdx], &group_tails, 1, 0, msgTagBase,
                                              SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        _dataCols[validIdx] = std::move(new_valid);
        clearInvalidEntries(msgTagBase);
    } else {
        _dataCols[validIdx] = std::move(group_tails);
        clearInvalidEntries(msgTagBase);
    }
}

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

    const bool BASELINE = DbConf::BASELINE_MODE;
    const bool APPROX_COMPACT = DbConf::DISABLE_PRECISE_COMPACTION;
    const bool PRECISE_COMPACT = !BASELINE && !APPROX_COMPACT;

    std::vector<int64_t> &src = _dataCols[fieldIdx];
    if (src.size() != n || heads.size() != n) {
        Log::e("Size mismatch: fieldData={}, heads={}, n={}", src.size(), heads.size(), n);
        return;
    }

    std::vector<int64_t> vs = src;
    std::vector<int64_t> bs_bool = heads;
    const int64_t XOR_MASK = Comm::rank();

    int validIdx = colNum() + VALID_COL_OFFSET;
    if (BASELINE || APPROX_COMPACT) {
        auto &valid = _dataCols[validIdx];

        bs_bool = BoolAndBatchOperator(&bs_bool, &valid, 1, 0, msgTagBase,
                                       SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        std::vector<int64_t> max_vals(n, (1LL << (bitlen - 1)) - 1);
        vs = BoolMutexBatchOperator(&vs, &max_vals, &valid, bitlen, 0, msgTagBase,
                                    SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    }

    for (int delta = 1; delta < static_cast<int>(n); delta <<= 1) {
        const int m = static_cast<int>(n) - delta;

        std::vector<int64_t> left_vals(m), right_vals(m);
        for (int i = 0; i < m; ++i) {
            left_vals[i] = vs[i];
            right_vals[i] = vs[i + delta];
        }

        if (BASELINE || APPROX_COMPACT) {
            auto &valid = _dataCols[validIdx];
            std::vector<int64_t> max_vals(m, (1LL << (bitlen - 1)) - 1), left_valid(m), right_valid(m);
            for (int i = 0; i < m; ++i) {
                left_valid[i] = valid[i];
                right_valid[i] = valid[i + delta];
            }
            left_vals = BoolMutexBatchOperator(&left_vals, &max_vals, &left_valid, bitlen, 0, msgTagBase,
                                               SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
            right_vals = BoolMutexBatchOperator(&right_vals, &max_vals, &right_valid, bitlen, 0, msgTagBase,
                                                SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        }

        auto cmp = BoolLessBatchOperator(&left_vals, &right_vals, bitlen, 0, msgTagBase,
                                         SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        auto cand = BoolMutexBatchOperator(&left_vals, &right_vals, &cmp, bitlen, 0, msgTagBase,
                                           SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        std::vector<int64_t> gate(m);
        for (int i = 0; i < m; ++i) gate[i] = (bs_bool[i + delta] ^ XOR_MASK);
        auto new_right = BoolMutexBatchOperator(&cand, &right_vals, &gate, bitlen, 0, msgTagBase,
                                                SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        for (int i = 0; i < m; ++i) vs[i + delta] = new_right[i];

        std::vector<int64_t> not_l(m), not_r(m);
        for (int i = 0; i < m; ++i) {
            not_l[i] = bs_bool[i] ^ XOR_MASK;
            not_r[i] = bs_bool[i + delta] ^ XOR_MASK;
        }
        auto and_out = BoolAndBatchOperator(&not_l, &not_r, 1, 0, msgTagBase,
                                            SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        for (int i = 0; i < m; ++i) bs_bool[i + delta] = (and_out[i] ^ XOR_MASK);
    }

    std::vector<int64_t> group_tails(n);
    for (size_t i = 0; i + 1 < n; ++i) group_tails[i] = heads[i + 1];

    if (PRECISE_COMPACT) {
        group_tails[n - 1] = Comm::rank();
    } else {
        auto &valid = _dataCols[validIdx];
        std::vector<int64_t> next_valid(n);
        for (size_t i = 0; i + 1 < n; ++i) next_valid[i] = valid[i + 1];
        next_valid[n - 1] = 0;

        std::vector<int64_t> not_next(n);
        for (size_t i = 0; i < n; ++i) not_next[i] = next_valid[i] ^ XOR_MASK;
        auto last_valid_tail = BoolAndBatchOperator(&valid, &not_next, 1, 0, msgTagBase,
                                                    SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        std::vector<int64_t> not_gt(n), not_lvt(n);
        for (size_t i = 0; i < n; ++i) {
            not_gt[i] = group_tails[i] ^ XOR_MASK;
            not_lvt[i] = last_valid_tail[i] ^ XOR_MASK;
        }
        auto and_not = BoolAndBatchOperator(&not_gt, &not_lvt, 1, 0, msgTagBase,
                                            SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        for (size_t i = 0; i < n; ++i) group_tails[i] = (and_not[i] ^ XOR_MASK);
    }

    const std::string outName = alias.empty() ? ("min_" + fieldName) : alias;
    _fieldNames.reserve(_fieldNames.size() + 1);
    _fieldWidths.reserve(_fieldWidths.size() + 1);
    _dataCols.reserve(_dataCols.size() + 1);

    const int insertPos = colNum() - 2;
    _fieldNames.insert(_fieldNames.begin() + insertPos, outName);
    _fieldWidths.insert(_fieldWidths.begin() + insertPos, bitlen);
    _dataCols.insert(_dataCols.begin() + insertPos, vs);

    validIdx = colNum() + VALID_COL_OFFSET;
    if (BASELINE) {
        auto new_valid = BoolAndBatchOperator(&_dataCols[validIdx], &group_tails, 1, 0, msgTagBase,
                                              SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        _dataCols[validIdx] = std::move(new_valid);
    } else if (APPROX_COMPACT) {
        auto new_valid = BoolAndBatchOperator(&_dataCols[validIdx], &group_tails, 1, 0, msgTagBase,
                                              SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        _dataCols[validIdx] = std::move(new_valid);
        clearInvalidEntries(msgTagBase);
    } else {
        _dataCols[validIdx] = std::move(group_tails);
        clearInvalidEntries(msgTagBase);
    }
}

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

    const bool APPROX_COMPACT = DbConf::DISABLE_PRECISE_COMPACTION;
    const int64_t rank = Comm::rank();
    int validIdx = colNum() + VALID_COL_OFFSET;

    std::vector<int64_t> bs_bool = heads;
    std::vector<int64_t> vs = src;

    if (APPROX_COMPACT) {
        bs_bool = BoolAndBatchOperator(&bs_bool, &_dataCols[validIdx], 1, 0, msgTagBase,
                                       SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        std::vector<int64_t> max_vals(n, (1LL << (bitlen - 1)) - 1);
        vs = BoolMutexBatchOperator(&vs, &max_vals, &_dataCols[validIdx], bitlen, 0, msgTagBase,
                                    SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    }

    const int lessStride = BoolLessBatchOperator::tagStride();
    const int mutexStride = BoolMutexBatchOperator::tagStride();
    const int andStride = BoolAndBatchOperator::tagStride();
    const int taskStride = lessStride + mutexStride + mutexStride + andStride;

    int tagCursorBase = msgTagBase;
    const int64_t NOT_mask = rank;

    for (int delta = 1; delta < static_cast<int>(n); delta <<= 1) {
        const int totalPairs = static_cast<int>(n) - delta;
        if (totalPairs <= 0) break;

        const int numBatches = (totalPairs + batchSize - 1) / batchSize;

        using Pair = std::pair<std::vector<int64_t>, std::vector<int64_t> >;

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
                for (int i = 0; i < len; ++i) {
                    const int idxL = start + i;
                    const int idxR = idxL + delta;
                    left_vals[i] = vs[idxL];
                    right_vals[i] = vs[idxR];
                }

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

                auto cmp = BoolLessBatchOperator(&left_vals, &right_vals, bitlen,
                                                 0, lessTag,
                                                 SecureOperator::NO_CLIENT_COMPUTE)
                        .execute()->_zis;

                auto cand = BoolMutexBatchOperator(&left_vals, &right_vals, &cmp,
                                                   bitlen, 0, mtx1Tag,
                                                   SecureOperator::NO_CLIENT_COMPUTE)
                        .execute()->_zis;

                std::vector<int64_t> not_bR(len);
                for (int i = 0; i < len; ++i) {
                    not_bR[i] = bs_bool[start + i + delta] ^ NOT_mask;
                }
                auto new_right = BoolMutexBatchOperator(&cand, &right_vals, &not_bR,
                                                        bitlen, 0, mtx2Tag,
                                                        SecureOperator::NO_CLIENT_COMPUTE)
                        .execute()->_zis;

                std::vector<int64_t> not_bL(len);
                for (int i = 0; i < len; ++i) {
                    not_bL[i] = bs_bool[start + i] ^ NOT_mask;
                    not_bR[i] = bs_bool[start + i + delta] ^ NOT_mask;
                }
                auto and_out = BoolAndBatchOperator(&not_bL, &not_bR,
                                                    1, 0, andTag,
                                                    SecureOperator::NO_CLIENT_COMPUTE)
                        .execute()->_zis;
                std::vector<int64_t> new_b(len);
                for (int i = 0; i < len; ++i) new_b[i] = and_out[i] ^ NOT_mask;

                return Pair{std::move(new_right), std::move(new_b)};
            });
        }

        tagCursorBase += numBatches * taskStride;

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

    std::vector<int64_t> group_tails(n);
    for (size_t i = 0; i + 1 < n; ++i) group_tails[i] = heads[i + 1];

    if (APPROX_COMPACT) {
        auto &valid = _dataCols[validIdx];
        std::vector<int64_t> next_valid(n);
        for (size_t i = 0; i + 1 < n; ++i) next_valid[i] = valid[i + 1];
        next_valid[n - 1] = 0;

        std::vector<int64_t> not_next(n);
        for (size_t i = 0; i < n; ++i) not_next[i] = next_valid[i] ^ rank;

        auto last_valid_tail = BoolAndBatchOperator(&valid, &not_next, 1, 0, msgTagBase,
                                                    SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

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

    const std::string outName = alias.empty() ? ("min_" + fieldName) : alias;

    _fieldNames.reserve(_fieldNames.size() + 1);
    _fieldWidths.reserve(_fieldWidths.size() + 1);
    _dataCols.reserve(_dataCols.size() + 1);

    const int insertPos = colNum() - 2;
    _fieldNames.insert(_fieldNames.begin() + insertPos, outName);
    _fieldWidths.insert(_fieldWidths.begin() + insertPos, bitlen);
    _dataCols.insert(_dataCols.begin() + insertPos, std::move(vs));

    validIdx = colNum() + VALID_COL_OFFSET;
    if (APPROX_COMPACT) {
        auto new_valid = BoolAndBatchOperator(&_dataCols[validIdx], &group_tails, 1, 0, msgTagBase,
                                              SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        _dataCols[validIdx] = std::move(new_valid);
        clearInvalidEntries(msgTagBase);
    } else {
        _dataCols[validIdx] = std::move(group_tails);
        clearInvalidEntries(msgTagBase);
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

    std::vector<int64_t> result;
    if (DbConf::BASELINE_MODE || Conf::DISABLE_MULTI_THREAD) {
        result = std::move(collected[0]);
        for (int i = 1; i < n; i++) {
            result = BoolAndBatchOperator(&result, &collected[i], 1, 0, msgTagBase,
                                          SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        }
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
        result = std::move(andCols.front());
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
    const size_t n = fieldNames.size();
    std::vector<std::vector<int64_t> > collected(n);

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
        result = std::move(andCols.front());
    }

    if (!DbConf::DISABLE_PRECISE_COMPACTION) {
        _dataCols[validColIndex] = std::move(result);
        if (clear) {
            clearInvalidEntries(msgTagBase);
        }
    } else {
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
    clearInvalidEntries(true, msgTagBase);
}

void View::clearInvalidEntries(bool doSort, int msgTagBase) {
    if (DbConf::BASELINE_MODE || DbConf::NO_COMPACTION) {
        return;
    }
    if (doSort) {
        sort(VALID_COL_NAME, false, msgTagBase);
    }
    int64_t sumShare = 0, sumShare1;
    if (Conf::DISABLE_MULTI_THREAD || Conf::BATCH_SIZE <= 0) {
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
        for (int b = 63; b > 0; --b) {
            int64_t bit_i = bits[b];
            int64_t bit_o = 0;

            auto s = Comm::serverSendAsync(bit_i, 1, msgTagBase);
            auto r = Comm::serverReceiveAsync(bit_o, 1, msgTagBase);
            Comm::wait(s);
            Comm::wait(r);

            int bit = static_cast<int>(bit_i ^ bit_o);
            if (bit) {
                msb = b;
                break;
            }
        }

        if (msb == 63) {
            return;
        }

        size_t n = rowNum();
        size_t keep = std::min(n, msb >= 0 ? static_cast<size_t>(1) << (msb + 1) : 2);

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


void View::minAndMaxSingleBatch(std::vector<int64_t> &heads,
                                const std::string &minFieldName,
                                const std::string &maxFieldName,
                                std::string minAlias,
                                std::string maxAlias,
                                int msgTagBase) {
    const size_t n = rowNum();
    if (n == 0) return;

    const int minIdx = colIndex(minFieldName);
    const int maxIdx = colIndex(maxFieldName);
    if (minIdx < 0 || maxIdx < 0) {
        Log::e("Field not found for minAndMax: min='{}' idx={}, max='{}' idx={}",
               minFieldName, minIdx, maxFieldName, maxIdx);
        return;
    }

    const int minBitlen = _fieldWidths[minIdx];
    const int maxBitlen = _fieldWidths[maxIdx];

    auto &min_src = _dataCols[minIdx];
    auto &max_src = _dataCols[maxIdx];
    if (min_src.size() != n || max_src.size() != n || heads.size() != n) {
        Log::e("Size mismatch in minAndMax: min={}, max={}, heads={}, n={}",
               min_src.size(), max_src.size(), heads.size(), n);
        return;
    }

    const bool BASELINE = DbConf::BASELINE_MODE;
    const bool APPROX_COMPACT = DbConf::DISABLE_PRECISE_COMPACTION;
    const bool PRECISE_COMPACT = !BASELINE && !APPROX_COMPACT;

    std::vector<int64_t> min_vs = min_src;
    std::vector<int64_t> max_vs = max_src;
    std::vector<int64_t> bs_bool = heads;

    const int64_t XOR_MASK = Comm::rank();

    int validIdx = colNum() + VALID_COL_OFFSET;
    if (BASELINE || APPROX_COMPACT) {
        auto &valid = _dataCols[validIdx];

        bs_bool = BoolAndBatchOperator(&bs_bool, &valid, 1, 0, msgTagBase,
                                       SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        std::vector<int64_t> max_vals_min(n, (1LL << (minBitlen - 1)) - 1);
        min_vs = BoolMutexBatchOperator(&min_vs, &max_vals_min, &valid, minBitlen, 0, msgTagBase,
                                        SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        std::vector<int64_t> min_vals_max(n, 0);
        max_vs = BoolMutexBatchOperator(&max_vs, &min_vals_max, &valid, maxBitlen, 0, msgTagBase,
                                        SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    }

    const int tag_off_less = BoolLessBatchOperator::tagStride();
    const int tag_off_and = BoolAndBatchOperator::tagStride();
    const int tag_off_mux = BoolMutexBatchOperator::tagStride();

    for (int delta = 1, round = 0; delta < (int) n; delta <<= 1, ++round) {
        const int m = (int) n - delta;
        if (m <= 0) break;

        std::vector<int64_t> min_L(m), min_R(m), max_L(m), max_R(m);
        for (int i = 0; i < m; ++i) {
            min_L[i] = min_vs[i];
            min_R[i] = min_vs[i + delta];
            max_L[i] = max_vs[i];
            max_R[i] = max_vs[i + delta];
        }

        if (BASELINE || APPROX_COMPACT) {
            auto &valid = _dataCols[validIdx];
            std::vector<int64_t> max_vals_min(m, (1LL << (minBitlen - 1)) - 1), min_vals_max(m, 0);
            std::vector<int64_t> left_valid(m), right_valid(m);
            for (int i = 0; i < m; ++i) {
                left_valid[i] = valid[i];
                right_valid[i] = valid[i + delta];
            }
            min_L = BoolMutexBatchOperator(&min_L, &max_vals_min, &left_valid, minBitlen, 0, msgTagBase,
                                           SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
            min_R = BoolMutexBatchOperator(&min_R, &max_vals_min, &right_valid, minBitlen, 0, msgTagBase,
                                           SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
            max_L = BoolMutexBatchOperator(&max_L, &min_vals_max, &left_valid, maxBitlen, 0, msgTagBase,
                                           SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
            max_R = BoolMutexBatchOperator(&max_R, &min_vals_max, &right_valid, maxBitlen, 0, msgTagBase,
                                           SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        }

        auto less_min = BoolLessBatchOperator(&min_L, &min_R, minBitlen, 0,
                                              msgTagBase + round * (2 * tag_off_less + tag_off_and + 4 * tag_off_mux),
                                              SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        auto less_max = BoolLessBatchOperator(&max_L, &max_R, maxBitlen, 0,
                                              msgTagBase + round * (2 * tag_off_less + tag_off_and + 4 * tag_off_mux) +
                                              tag_off_less,
                                              SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        auto min_cand = BoolMutexBatchOperator(&min_L, &min_R, &less_min, minBitlen, 0,
                                               msgTagBase + round * (2 * tag_off_less + tag_off_and + 4 * tag_off_mux) +
                                               2 *
                                               tag_off_less,
                                               SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        auto max_cand = BoolMutexBatchOperator(&max_R, &max_L, &less_max, maxBitlen, 0,
                                               msgTagBase + round * (2 * tag_off_less + tag_off_and + 4 * tag_off_mux) +
                                               2 *
                                               tag_off_less + tag_off_mux,
                                               SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        std::vector<int64_t> gate(m);
        for (int i = 0; i < m; ++i) gate[i] = (bs_bool[i + delta] ^ XOR_MASK);

        auto min_new_right = BoolMutexBatchOperator(&min_cand, &min_R, &gate, minBitlen, 0,
                                                    msgTagBase + round * (
                                                        2 * tag_off_less + tag_off_and + 4 * tag_off_mux)
                                                    + 2 * tag_off_less + 2 * tag_off_mux,
                                                    SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        auto max_new_right = BoolMutexBatchOperator(&max_cand, &max_R, &gate, maxBitlen, 0,
                                                    msgTagBase + round * (
                                                        2 * tag_off_less + tag_off_and + 4 * tag_off_mux)
                                                    + 2 * tag_off_less + 3 * tag_off_mux,
                                                    SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        for (int i = 0; i < m; ++i) {
            min_vs[i + delta] = min_new_right[i];
            max_vs[i + delta] = max_new_right[i];
        }

        std::vector<int64_t> not_l(m), not_r(m);
        for (int i = 0; i < m; ++i) {
            not_l[i] = bs_bool[i] ^ XOR_MASK;
            not_r[i] = bs_bool[i + delta] ^ XOR_MASK;
        }
        auto and_out = BoolAndBatchOperator(&not_l, &not_r, 1, 0,
                                            msgTagBase + round * (2 * tag_off_less + tag_off_and + 4 * tag_off_mux) + 2
                                            *
                                            tag_off_less + 4 * tag_off_mux,
                                            SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        for (int i = 0; i < m; ++i) bs_bool[i + delta] = and_out[i] ^ XOR_MASK;
    }

    std::vector<int64_t> group_tails(n);
    for (size_t i = 0; i + 1 < n; ++i) group_tails[i] = heads[i + 1];

    if (PRECISE_COMPACT) {
        group_tails[n - 1] = Comm::rank();
    } else {
        auto &valid = _dataCols[validIdx];
        std::vector<int64_t> next_valid(n);
        for (size_t i = 0; i + 1 < n; ++i) next_valid[i] = valid[i + 1];
        next_valid[n - 1] = 0;

        std::vector<int64_t> not_next(n);
        for (size_t i = 0; i < n; ++i) not_next[i] = next_valid[i] ^ XOR_MASK;
        auto last_valid_tail = BoolAndBatchOperator(&valid, &not_next, 1, 0, msgTagBase,
                                                    SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        std::vector<int64_t> not_gt(n), not_lvt(n);
        for (size_t i = 0; i < n; ++i) {
            not_gt[i] = group_tails[i] ^ XOR_MASK;
            not_lvt[i] = last_valid_tail[i] ^ XOR_MASK;
        }
        auto and_not = BoolAndBatchOperator(&not_gt, &not_lvt, 1, 0, msgTagBase,
                                            SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        for (size_t i = 0; i < n; ++i) group_tails[i] = (and_not[i] ^ XOR_MASK);
    }

    const std::string minOutName = minAlias.empty() ? ("min_" + minFieldName) : minAlias;
    const std::string maxOutName = maxAlias.empty() ? ("max_" + maxFieldName) : maxAlias;

    const int insertPos = colNum() - 2;
    _fieldNames.insert(_fieldNames.begin() + insertPos, minOutName);
    _fieldWidths.insert(_fieldWidths.begin() + insertPos, minBitlen);
    _dataCols.insert(_dataCols.begin() + insertPos, std::move(min_vs));

    _fieldNames.insert(_fieldNames.begin() + insertPos + 1, maxOutName);
    _fieldWidths.insert(_fieldWidths.begin() + insertPos + 1, maxBitlen);
    _dataCols.insert(_dataCols.begin() + insertPos + 1, std::move(max_vs));

    validIdx = colNum() + VALID_COL_OFFSET;
    if (BASELINE) {
        auto new_valid = BoolAndBatchOperator(&_dataCols[validIdx], &group_tails, 1, 0, msgTagBase,
                                              SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        _dataCols[validIdx] = std::move(new_valid);
    } else if (APPROX_COMPACT) {
        auto new_valid = BoolAndBatchOperator(&_dataCols[validIdx], &group_tails, 1, 0, msgTagBase,
                                              SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        _dataCols[validIdx] = std::move(new_valid);
        clearInvalidEntries(msgTagBase);
    } else {
        _dataCols[validIdx] = std::move(group_tails);
        clearInvalidEntries(msgTagBase);
    }
}

void View::minAndMaxMultiBatches(std::vector<int64_t> &heads,
                                 const std::string &minFieldName,
                                 const std::string &maxFieldName,
                                 std::string minAlias,
                                 std::string maxAlias,
                                 int msgTagBase) {
    const size_t n = rowNum();
    if (n == 0) return;

    const int minIdx = colIndex(minFieldName);
    const int maxIdx = colIndex(maxFieldName);
    if (minIdx < 0 || maxIdx < 0) {
        Log::e("Field not found for minAndMax: min='{}' idx={}, max='{}' idx={}",
               minFieldName, minIdx, maxFieldName, maxIdx);
        return;
    }

    const int minBitlen = _fieldWidths[minIdx];
    const int maxBitlen = _fieldWidths[maxIdx];

    auto &min_src = _dataCols[minIdx];
    auto &max_src = _dataCols[maxIdx];
    if (min_src.size() != n || max_src.size() != n || heads.size() != n) {
        Log::e("Size mismatch in minAndMax multi: min={}, max={}, heads={}, n={}",
               min_src.size(), max_src.size(), heads.size(), n);
        return;
    }

    const int batchSize = Conf::BATCH_SIZE;
    if (n <= (size_t) batchSize) {
        minAndMaxSingleBatch(heads, minFieldName, maxFieldName, std::move(minAlias), std::move(maxAlias), msgTagBase);
        return;
    }

    const bool APPROX_COMPACT = DbConf::DISABLE_PRECISE_COMPACTION;
    const bool PRECISE_COMPACT = !APPROX_COMPACT;

    std::vector<int64_t> min_vs = min_src;
    std::vector<int64_t> max_vs = max_src;
    std::vector<int64_t> bs_bool = heads;
    const int64_t NOT_mask = Comm::rank();

    int validIdx = colNum() + VALID_COL_OFFSET;
    if (APPROX_COMPACT) {
        bs_bool = BoolAndBatchOperator(&bs_bool, &_dataCols[validIdx], 1, 0, msgTagBase,
                                       SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        std::vector<int64_t> max_vals_min(n, (1LL << (minBitlen - 1)) - 1);
        min_vs = BoolMutexBatchOperator(&min_vs, &max_vals_min, &_dataCols[validIdx], minBitlen, 0, msgTagBase,
                                        SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        std::vector<int64_t> min_vals_max(n, 0);
        max_vs = BoolMutexBatchOperator(&max_vs, &min_vals_max, &_dataCols[validIdx], maxBitlen, 0, msgTagBase,
                                        SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    }

    const int lessStride = BoolLessBatchOperator::tagStride();
    const int mutexStride = BoolMutexBatchOperator::tagStride();
    const int andStride = BoolAndBatchOperator::tagStride();
    const int taskStride = 2 * lessStride + 4 * mutexStride + andStride;

    int tagCursorBase = msgTagBase;

    for (int delta = 1; delta < (int) n; delta <<= 1) {
        const int totalPairs = (int) n - delta;
        if (totalPairs <= 0) break;

        const int numBatches = (totalPairs + batchSize - 1) / batchSize;

        using Triple = std::tuple<std::vector<int64_t>, std::vector<int64_t>, std::vector<int64_t> >;
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

                std::vector<int64_t> min_left(len), min_right(len);
                std::vector<int64_t> max_left(len), max_right(len);
                std::vector<int64_t> not_bL(len), not_bR(len);

                for (int i = 0; i < len; ++i) {
                    const int idxL = start + i;
                    const int idxR = idxL + delta;
                    min_left[i] = min_vs[idxL];
                    min_right[i] = min_vs[idxR];
                    max_left[i] = max_vs[idxL];
                    max_right[i] = max_vs[idxR];
                    not_bL[i] = bs_bool[idxL] ^ NOT_mask;
                    not_bR[i] = bs_bool[idxR] ^ NOT_mask;
                }

                if (APPROX_COMPACT) {
                    auto &valid = _dataCols[validIdx];
                    std::vector<int64_t> max_vals_min(len, (1LL << (minBitlen - 1)) - 1), min_vals_max(len, 0);
                    std::vector<int64_t> left_valid(len), right_valid(len);
                    for (int i = 0; i < len; ++i) {
                        left_valid[i] = valid[start + i];
                        right_valid[i] = valid[start + i + delta];
                    }
                    min_left = BoolMutexBatchOperator(&min_left, &max_vals_min, &left_valid, minBitlen, 0, minLessTag,
                                                      SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
                    min_right = BoolMutexBatchOperator(&min_right, &max_vals_min, &right_valid, minBitlen, 0,
                                                       minLessTag,
                                                       SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
                    max_left = BoolMutexBatchOperator(&max_left, &min_vals_max, &left_valid, maxBitlen, 0, maxLessTag,
                                                      SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
                    max_right = BoolMutexBatchOperator(&max_right, &min_vals_max, &right_valid, maxBitlen, 0,
                                                       maxLessTag,
                                                       SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
                }

                auto min_cmp = BoolLessBatchOperator(&min_left, &min_right, minBitlen, 0,
                                                     minLessTag, SecureOperator::NO_CLIENT_COMPUTE)
                        .execute()->_zis;
                auto max_cmp = BoolLessBatchOperator(&max_left, &max_right, maxBitlen, 0,
                                                     maxLessTag, SecureOperator::NO_CLIENT_COMPUTE)
                        .execute()->_zis;

                auto min_cand = BoolMutexBatchOperator(&min_left, &min_right, &min_cmp,
                                                       minBitlen, 0, minMtx1Tag,
                                                       SecureOperator::NO_CLIENT_COMPUTE)
                        .execute()->_zis;
                auto max_cand = BoolMutexBatchOperator(&max_right, &max_left, &max_cmp,
                                                       maxBitlen, 0, maxMtx1Tag,
                                                       SecureOperator::NO_CLIENT_COMPUTE)
                        .execute()->_zis;

                auto min_new_right = BoolMutexBatchOperator(&min_cand, &min_right, &not_bR,
                                                            minBitlen, 0, minMtx2Tag,
                                                            SecureOperator::NO_CLIENT_COMPUTE)
                        .execute()->_zis;
                auto max_new_right = BoolMutexBatchOperator(&max_cand, &max_right, &not_bR,
                                                            maxBitlen, 0, maxMtx2Tag,
                                                            SecureOperator::NO_CLIENT_COMPUTE)
                        .execute()->_zis;

                auto and_out = BoolAndBatchOperator(&not_bL, &not_bR, 1, 0, andTag,
                                                    SecureOperator::NO_CLIENT_COMPUTE)
                        .execute()->_zis;
                std::vector<int64_t> new_b(len);
                for (int i = 0; i < len; ++i) new_b[i] = and_out[i] ^ NOT_mask;

                return Triple{std::move(min_new_right), std::move(max_new_right), std::move(new_b)};
            });
        }

        tagCursorBase += numBatches * taskStride;

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

    std::vector<int64_t> group_tails(n);
    for (size_t i = 0; i + 1 < n; ++i) group_tails[i] = heads[i + 1];

    if (PRECISE_COMPACT) {
        group_tails[n - 1] = Comm::rank();
    } else {
        auto &valid = _dataCols[validIdx];
        std::vector<int64_t> next_valid(n);
        for (size_t i = 0; i + 1 < n; ++i) next_valid[i] = valid[i + 1];
        next_valid[n - 1] = 0;

        std::vector<int64_t> not_next(n);
        for (size_t i = 0; i < n; ++i) not_next[i] = next_valid[i] ^ NOT_mask;

        auto last_valid_tail = BoolAndBatchOperator(&valid, &not_next, 1, 0, msgTagBase,
                                                    SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        std::vector<int64_t> not_gt(n), not_lvt(n);
        for (size_t i = 0; i < n; ++i) {
            not_gt[i] = group_tails[i] ^ NOT_mask;
            not_lvt[i] = last_valid_tail[i] ^ NOT_mask;
        }
        auto and_not = BoolAndBatchOperator(&not_gt, &not_lvt, 1, 0, msgTagBase,
                                            SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        for (size_t i = 0; i < n; ++i) group_tails[i] = and_not[i] ^ NOT_mask;
    }

    const std::string minOutName = minAlias.empty() ? ("min_" + minFieldName) : minAlias;
    const std::string maxOutName = maxAlias.empty() ? ("max_" + maxFieldName) : maxAlias;

    const int insertPos = colNum() - 2;
    _fieldNames.insert(_fieldNames.begin() + insertPos, minOutName);
    _fieldWidths.insert(_fieldWidths.begin() + insertPos, minBitlen);
    _dataCols.insert(_dataCols.begin() + insertPos, std::move(min_vs));

    _fieldNames.insert(_fieldNames.begin() + insertPos + 1, maxOutName);
    _fieldWidths.insert(_fieldWidths.begin() + insertPos + 1, maxBitlen);
    _dataCols.insert(_dataCols.begin() + insertPos + 1, std::move(max_vs));

    validIdx = colNum() + VALID_COL_OFFSET;
    if (APPROX_COMPACT) {
        auto new_valid = BoolAndBatchOperator(&_dataCols[validIdx], &group_tails, 1, 0, msgTagBase,
                                              SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        _dataCols[validIdx] = std::move(new_valid);
        clearInvalidEntries(msgTagBase);
    } else {
        _dataCols[validIdx] = std::move(group_tails);
        clearInvalidEntries(msgTagBase);
    }
}

std::vector<int64_t> View::groupBy(const std::string &groupField, int msgTagBase) {
    return groupBy(groupField, true, msgTagBase);
}

std::vector<int64_t> View::groupBy(const std::string &groupField, bool doSort, int msgTagBase) {
    size_t n = rowNum();
    if (n == 0) {
        return std::vector<int64_t>();
    }

    if (doSort) {
        if (!DbConf::BASELINE_MODE && !DbConf::DISABLE_PRECISE_COMPACTION) {
            sort(groupField, true, msgTagBase);
        } else {
            std::vector<std::string> sortFields;
            std::vector<bool> ascending;
            sortFields.push_back(VALID_COL_NAME);
            ascending.push_back(false);
            sortFields.push_back(groupField);
            ascending.push_back(true);
            sort(sortFields, ascending, msgTagBase);
            clearInvalidEntries(false, msgTagBase);
        }
    }

    if (Conf::BATCH_SIZE <= 0 || Conf::DISABLE_MULTI_THREAD) {
        return groupBySingleBatch(groupField, msgTagBase);
    } else {
        return groupByMultiBatches(groupField, msgTagBase);
    }
}

std::vector<int64_t> View::groupBy(const std::vector<std::string> &groupFields, int msgTagBase) {
    size_t n = rowNum();
    if (n == 0 || groupFields.empty()) {
        return std::vector<int64_t>();
    }

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
            ascending.push_back(true);
        }
        sort(sortFields, ascending, msgTagBase);
        clearInvalidEntries(false, msgTagBase);
    }
    if (Conf::BATCH_SIZE <= 0 || Conf::DISABLE_MULTI_THREAD) {
        return groupBySingleBatch(groupFields, msgTagBase);
    } else {
        return groupByMultiBatches(groupFields, msgTagBase);
    }
}

void View::count(std::vector<std::string> &groupFields, std::vector<int64_t> &heads, std::string alias,
                 int msgTagBase) {
    count(groupFields, heads, std::move(alias), true, msgTagBase);
}

void View::count(std::vector<std::string> &groupFields, std::vector<int64_t> &heads, std::string alias, bool compress,
                 int msgTagBase) {
    size_t pos = groupFields[0].find('.');
    std::string matchedTable;
    if (groupFields.size() == 1 && pos != std::string::npos) {
        std::string fieldTable = groupFields[0].substr(0, pos);
        for (int i = 0; i < _fieldNames.size(); i++) {
            if (_fieldNames[i] == OUTER_MATCH_PREFIX + fieldTable) {
                matchedTable = fieldTable;
                break;
            }
        }
    }
    const int oldColNum = static_cast<int>(colNum());
    const int validIdx = oldColNum + VALID_COL_OFFSET;
    const int paddingIdx = oldColNum + PADDING_COL_OFFSET;

    std::vector<int> groupIdx;
    groupIdx.reserve(groupFields.size());
    for (const auto &name: groupFields) {
        int idx = colIndex(name);
        groupIdx.push_back(idx);
    }

    const size_t keep = groupIdx.size() + 2;
    std::vector<std::vector<int64_t> > newDataCols;
    std::vector<std::string> newFieldNames;
    std::vector<int> newFieldWidths;
    newDataCols.reserve(keep);
    newFieldNames.reserve(keep);
    newFieldWidths.reserve(keep);

    for (int gi: groupIdx) {
        newDataCols.push_back(std::move(_dataCols[gi]));
        newFieldNames.push_back(std::move(_fieldNames[gi]));
        newFieldWidths.push_back(_fieldWidths[gi]);
    }

    if (!matchedTable.empty()) {
        int mc = colIndex(View::OUTER_MATCH_PREFIX + matchedTable);
        newDataCols.push_back(std::move(_dataCols[mc]));
        newFieldNames.emplace_back(std::move(_fieldNames[mc]));
        newFieldWidths.push_back(1);
    }
    newDataCols.push_back(std::move(_dataCols[validIdx]));
    newDataCols.push_back(std::move(_dataCols[paddingIdx]));
    newFieldNames.emplace_back(VALID_COL_NAME);
    newFieldNames.emplace_back(PADDING_COL_NAME);
    newFieldWidths.push_back(1);
    newFieldWidths.push_back(1);

    _dataCols = std::move(newDataCols);
    _fieldNames = std::move(newFieldNames);
    _fieldWidths = std::move(newFieldWidths);

    if (Conf::BATCH_SIZE <= 0 || Conf::DISABLE_MULTI_THREAD) {
        countSingleBatch(heads, alias, msgTagBase, matchedTable, compress);
    } else {
        countMultiBatches(heads, alias, msgTagBase, matchedTable, compress);
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
    minAndMax(heads, fieldName, fieldName, std::move(minAlias), std::move(maxAlias), msgTagBase);
}

void View::minAndMax(std::vector<int64_t> &heads,
                     const std::string &minFieldName,
                     const std::string &maxFieldName,
                     std::string minAlias,
                     std::string maxAlias,
                     int msgTagBase) {
    if (Conf::DISABLE_MULTI_THREAD || Conf::BATCH_SIZE <= 0) {
        minAndMaxSingleBatch(heads, minFieldName, maxFieldName, std::move(minAlias), std::move(maxAlias), msgTagBase);
    } else {
        minAndMaxMultiBatches(heads, minFieldName, maxFieldName, std::move(minAlias), std::move(maxAlias), msgTagBase);
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

    int tags_per_record = BoolEqualBatchOperator::tagStride() +
                          BoolToArithBatchOperator::tagStride() +
                          ArithMutexBatchOperator::tagStride(64) +
                          ArithMultiplyBatchOperator::tagStride(64);

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

void View::sort(const std::vector<std::string> &orderFields, const std::vector<bool> &ascendingOrders, int msgTagBase) {
    if (orderFields.empty()) {
        return;
    }

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

    if (orderFields.size() == 1) {
        return sortTagStride();
    }

    int base_stride = ((rowNum() / 2 + Conf::BATCH_SIZE - 1) / Conf::BATCH_SIZE) * (colNum() - 1) *
                      BoolMutexBatchOperator::tagStride();

    int multi_col_factor = static_cast<int>(orderFields.size() * 2);

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
            const int baseTagStride = BoolLessBatchOperator::tagStride();
            const int eqTagStride = BoolEqualBatchOperator::tagStride();
            const int andTagStride = BoolAndBatchOperator::tagStride();
            const int mutexTagStride = BoolMutexBatchOperator::tagStride();

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

                    std::vector<int64_t> xs, ys;
                    xs.reserve(cnt);
                    ys.reserve(cnt);

                    for (int t = start; t < end; ++t) {
                        xs.push_back(_dataCols[orderFieldIndices[0]][xIdx[t]]);
                        ys.push_back(_dataCols[orderFieldIndices[0]][yIdx[t]]);
                    }

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

                        lts = BoolMutexBatchOperator(&lts_i, &lts, &eqs, 1, 0, batchTagBase,
                                                     SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

                        std::vector<int64_t> eqs_i = BoolEqualBatchOperator(
                            &xs, &ys, _fieldWidths[orderFieldIndices[col]], 0,
                            batchTagBase,
                            SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

                        eqs = BoolAndBatchOperator(&eqs, &eqs_i, 1, 0, batchTagBase,
                                                   SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
                    }

                    for (int t = 0; t < cnt; ++t) {
                        if (!dirs[start + t]) {
                            lts[t] ^= Comm::rank();
                        }
                    }

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
