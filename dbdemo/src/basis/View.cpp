//
// Created by 杜建璋 on 25-4-25.
//

#include "../../include/basis/View.h"

#include <numeric>

#include "../../../../../../../opt/homebrew/Cellar/boost/1.87.0/include/boost/config/detail/posix_features.hpp"
#include "compute/batch/bool/BoolAndBatchOperator.h"
#include "compute/batch/bool/BoolEqualBatchOperator.h"
#include "compute/batch/bool/BoolLessBatchOperator.h"
#include "compute/batch/bool/BoolMutexBatchOperator.h"
#include "compute/batch/bool/BoolToArithBatchOperator.h"
#include "conf/DbConf.h"
#include "parallel/ThreadPoolSupport.h"
#include "secret/Secrets.h"
#include "utils/Log.h"
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

void View::sort(const std::string &orderField, bool ascendingOrder, int msgTagOffset) {
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
    bitonicSort(orderField, ascendingOrder, msgTagOffset);
    if (n < _dataCols[0].size()) {
        for (auto &v: _dataCols) {
            v.resize(n);
        }
    }
}

void View::fac1B(std::vector<std::string> &fieldNames, std::vector<ComparatorType> &comparatorTypes,
                 std::vector<int64_t> &constShares) {
    size_t n = fieldNames.size();
    std::vector<std::vector<int64_t> > collected(n);

    std::vector<std::future<std::vector<int64_t> > > futures(n);
    int tagOffset = std::max(BoolLessBatchOperator::msgTagCount(), BoolAndBatchOperator::msgTagCount());

    for (int i = 0; i < n; i++) {
        futures[i] = ThreadPoolSupport::submit([&, i] {
            std::vector<int64_t> currentCondition;
            int colIndex = 0;
            for (int j = 0; j < _fieldNames.size(); j++) {
                if (_fieldNames[j] == fieldNames[i]) {
                    colIndex = j;
                    break;
                }
            }
            auto extendedConstShares = std::vector<int64_t>(_dataCols[colIndex].size(), constShares[i]);
            auto ct = comparatorTypes[i];

            int startTag = tagOffset * i;
            switch (ct) {
                case GREATER:
                case LESS_EQ: {
                    currentCondition = BoolLessBatchOperator(&extendedConstShares, &_dataCols[colIndex],
                                                             _fieldWidths[colIndex], 0, startTag,
                                                             SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
                }
                    if (ct == LESS_EQ) {
                        for (auto &v: currentCondition) {
                            v = v ^ Comm::rank();
                        }
                    }
                    break;
                case LESS:
                case GREATER_EQ:
                    currentCondition = BoolLessBatchOperator(&_dataCols[colIndex], &extendedConstShares,
                                                             _fieldWidths[colIndex], 0, startTag,
                                                             SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
                    if (ct == GREATER_EQ) {
                        for (auto &v: currentCondition) {
                            v = v ^ Comm::rank();
                        }
                    }
                    break;
                case EQUALS:
                case NOT_EQUALS:
                    currentCondition = BoolEqualBatchOperator(&extendedConstShares, &_dataCols[colIndex],
                                                              _fieldWidths[colIndex], 0, startTag,
                                                              SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
                    if (ct == NOT_EQUALS) {
                        for (auto &v: currentCondition) {
                            v = v ^ Comm::rank();
                        }
                    }
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

    if (Conf::DISABLE_MULTI_THREAD) {
        _dataCols[validColIndex] = collected[0];
        for (int i = 1; i < n; i++) {
            _dataCols[validColIndex] = BoolAndBatchOperator(&collected[i], &_dataCols[validColIndex], 1,
                                                            0, 0,
                                                            SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        }
    } else {
        std::vector<std::vector<int64_t> > andCols = std::move(collected);
        int level = 0;
        while (andCols.size() > 1) {
            int m = static_cast<int>(andCols.size());
            int pairs = m / 2;
            int msgStride = BoolAndBatchOperator::msgTagCount();

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
                                level * msgStride + p, SecureOperator::NO_CLIENT_COMPUTE
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
        }

        if (!andCols.empty()) {
            _dataCols[validColIndex] = std::move(andCols.front());
        }
    }
}

void View::facNB(std::vector<std::string> &fieldNames, std::vector<ComparatorType> &comparatorTypes,
                 std::vector<int64_t> &constShares) {
    size_t n = fieldNames.size();
    std::vector<std::vector<int64_t> > collected(n);

    std::vector<std::future<std::vector<int64_t> > > futures(n);
    size_t data_size = _dataCols[0].size();
    int batchSize = Conf::BATCH_SIZE;
    int batchNum = (data_size + batchSize - 1) / batchSize;
    int tagOffset = std::max(BoolLessBatchOperator::msgTagCount(), BoolEqualBatchOperator::msgTagCount());

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
        int msgStride = BoolAndBatchOperator::msgTagCount();

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
                            level * msgStride + p, SecureOperator::NO_CLIENT_COMPUTE
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
}

void View::filterAndConditions(std::vector<std::string> &fieldNames, std::vector<ComparatorType> &comparatorTypes,
                               std::vector<int64_t> &constShares) {
    if (Conf::BATCH_SIZE <= 0 || Conf::DISABLE_MULTI_THREAD) {
        fac1B(fieldNames, comparatorTypes, constShares);
    } else {
        facNB(fieldNames, comparatorTypes, constShares);
    }

    clearInvalidEntries();
}

void View::clearInvalidEntries() {
    // sort view by valid column
    sort(VALID_COL_NAME, false, 0);

    int64_t sumShare = 0, sumShare1;
    if (Conf::DISABLE_MULTI_THREAD || Conf::BATCH_SIZE <= 0) {
        // compute valid num
        auto ta = BoolToArithBatchOperator(&_dataCols[_dataCols.size() + VALID_COL_OFFSET], 64, 0, 0,
                                           SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        sumShare = std::accumulate(ta.begin(), ta.end(), 0ll);
    } else {
        size_t batchSize = Conf::BATCH_SIZE;
        size_t n = _dataCols[0].size();
        size_t batchNum = (n + batchSize - 1) / batchSize;
        std::vector<std::future<std::vector<int64_t> > > futures(batchNum);

        for (int i = 0; i < batchNum; ++i) {
            futures[i] = ThreadPoolSupport::submit([this, i, batchSize, n] {
                auto &validCol = _dataCols[_dataCols.size() + VALID_COL_OFFSET];
                std::vector part(validCol.begin() + batchSize * i,
                                 validCol.begin() + std::min(batchSize * (i + 1), n));
                return BoolToArithBatchOperator(&part, 64, 0, BoolToArithBatchOperator::msgTagCount() * i,
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

    auto r0 = Comm::serverSendAsync(sumShare, 32, 0);
    auto r1 = Comm::serverReceiveAsync(sumShare1, 32, 0);
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

void View::addRedundantCols() {
    _fieldNames.emplace_back(View::VALID_COL_NAME);
    _fieldWidths.emplace_back(1);
    _dataCols.emplace_back(_dataCols[0].size(), Comm::rank());

    _fieldNames.emplace_back(View::PADDING_COL_NAME);
    _fieldWidths.emplace_back(1);
    _dataCols.emplace_back(_dataCols[0].size());
}

void View::bs1B(const std::string &orderField, bool ascendingOrder, int msgTagOffset) {
    auto n = _dataCols[0].size();
    auto &orderCol = _dataCols[colIndex(orderField)];
    auto &paddings = _dataCols[colNum() + PADDING_COL_OFFSET];
    int ofi = colIndex(orderField);
    auto &oc = _dataCols[ofi]; // order column
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
            zs = BoolLessBatchOperator(&xs, &ys, _fieldWidths[ofi], 0, msgTagOffset,
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

            zs = BoolMutexBatchOperator(&xs, &ys, &zs, _maxWidth, 0, msgTagOffset).
                    execute()->_zis;

            for (int i = 0; i < comparingCount; i++) {
                oc[xIdx[i]] = zs[i];
                oc[yIdx[i]] = zs[(colNum() - 1) * comparingCount + i];
            }

            // skip first for order column
            int pos = 1;
            for (int i = 0; i < colNum() - 1; i++) {
                if (i == ofi) {
                    continue;
                }
                auto &co = _dataCols[i];
                for (int m = 0; m < comparingCount; m++) {
                    co[xIdx[m]] = zs[pos * comparingCount + m];
                    co[yIdx[m]] = zs[(pos + colNum() - 1) * comparingCount + m];
                }
                pos++;
            }
        }
    }
}

void View::bsNB(const std::string &orderField, bool ascendingOrder, int msgTagOffset) {
    const int batchSize = Conf::BATCH_SIZE;
    const int n = static_cast<int>(_dataCols[0].size());
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
            const int tagStride = static_cast<int>(BoolMutexBatchOperator::msgTagCount() * (colNum() - 1));
            std::vector<std::future<void> > futures;
            futures.reserve(numBatches);
            for (int b = 0; b < numBatches; ++b) {
                futures.emplace_back(ThreadPoolSupport::submit([&,b]() {
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
                                                    msgTagOffset + tagStride * b,
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
                                                              msgTagOffset + tagStride * b +
                                                              BoolMutexBatchOperator::msgTagCount() * c).execute()->
                                    _zis;

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

void View::bitonicSort(const std::string &orderField, bool ascendingOrder, int msgTagOffset) {
    if (Conf::BATCH_SIZE <= 0 || Conf::DISABLE_MULTI_THREAD) {
        bs1B(orderField, ascendingOrder, msgTagOffset);
    } else {
        bsNB(orderField, ascendingOrder, msgTagOffset);
    }
}
