//
// Created by 杜建璋 on 25-4-25.
//

#include "../../include/basis/View.h"

#include "compute/batch/bool/BoolAndBatchOperator.h"
#include "compute/batch/bool/BoolLessBatchOperator.h"
#include "compute/batch/bool/BoolMutexBatchOperator.h"
#include "parallel/ThreadPoolSupport.h"
#include "secret/Secrets.h"
#include "utils/Log.h"
#include "utils/StringUtils.h"

View::View(std::string &tableName,
           std::vector<std::string> &fieldNames,
           std::vector<int> &fieldWidths) : Table(
    tableName, fieldNames, fieldWidths) {
    // for padding column and valid column
    _colNum += 2;
}

void View::sort(const std::string &orderField, bool ascendingOrder, int msgTagOffset) {
    size_t n = _dataCols[0].size();
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

void View::fa1B(std::vector<std::string> &fieldNames, std::vector<ComparatorType> &comparatorTypes,
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
                    auto gtv_ltv = BoolLessBatchOperator(&extendedConstShares, &_dataCols[colIndex],
                                                         _fieldWidths[colIndex], 0,
                                                         startTag).execute()->_zis;
                    for (auto &v: gtv_ltv) {
                        v = v ^ Comm::rank();
                    }
                    std::vector ltv(gtv_ltv.begin() + gtv_ltv.size() / 2, gtv_ltv.end());
                    std::vector gtv = std::move(gtv_ltv);
                    gtv.resize(gtv.size() / 2);

                    currentCondition = BoolAndBatchOperator(&gtv, &ltv, 1, 0, startTag,
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

    int validColIndex = _colNum + VALID_COL_OFFSET;
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

void View::faNB(std::vector<std::string> &fieldNames, std::vector<ComparatorType> &comparatorTypes,
                std::vector<int64_t> &constShares) {
    size_t n = fieldNames.size();
    std::vector<std::vector<int64_t> > collected(n);

    std::vector<std::future<std::vector<int64_t> > > futures(n);
    size_t data_size = _dataCols[0].size();
    int batch_size = Conf::BATCH_SIZE;
    int num_batches = (data_size + batch_size - 1) / batch_size;
    int tagOffset = std::max(BoolLessBatchOperator::msgTagCount(), BoolAndBatchOperator::msgTagCount());

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

            std::vector<std::future<std::vector<int64_t> > > batch_futures(num_batches);
            int startTag = tagOffset * num_batches * i;

            for (int b = 0; b < num_batches; ++b) {
                int start = b * batch_size;
                int end = std::min(start + batch_size, static_cast<int>(data_size));
                int current_batch_size = end - start;

                batch_futures[b] = ThreadPoolSupport::submit(
                    [&, start, end, ct, colIndex, startTag, b, current_batch_size]() {
                        std::vector<int64_t> batch_const(current_batch_size, constShares[i]);
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
                                auto gtv_ltv = BoolLessBatchOperator(&batch_const, &batch_data,
                                                                     _fieldWidths[colIndex], 0, batch_start_tag,
                                                                     SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
                                for (auto &v: gtv_ltv) {
                                    v ^= Comm::rank();
                                }

                                size_t half = gtv_ltv.size() / 2;
                                std::vector<int64_t> gtv(gtv_ltv.begin(), gtv_ltv.begin() + half);
                                std::vector<int64_t> ltv(gtv_ltv.begin() + half, gtv_ltv.end());

                                batch_result = BoolAndBatchOperator(&gtv, &ltv, 1, 0, batch_start_tag,
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

    int validColIndex = _colNum + VALID_COL_OFFSET;
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

void View::filterAnd(std::vector<std::string> &fieldNames, std::vector<ComparatorType> &comparatorTypes,
                     std::vector<int64_t> &constShares) {
    if (Conf::BATCH_SIZE <= 0 || Conf::DISABLE_MULTI_THREAD) {
        fa1B(fieldNames, comparatorTypes, constShares);
    } else {
        faNB(fieldNames, comparatorTypes, constShares);
    }
}

void View::bs1B(const std::string &orderField, bool ascendingOrder, int msgTagOffset) {
    auto n = _dataCols[0].size();
    auto &col = getColData(orderField);
    for (int k = 2; k <= n; k *= 2) {
        for (int j = k / 2; j > 0; j /= 2) {
            std::vector<int64_t> xs;
            std::vector<int64_t> ys;
            std::vector<int64_t> xIdx;
            std::vector<int64_t> yIdx;
            std::vector<bool> ascs;
            int halfN = n / 2;
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
                auto &paddings = _dataCols[_colNum + PADDING_COL_OFFSET];
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
                xs.push_back(col[i]);
                xIdx.push_back(i);
                ys.push_back(col[l]);
                yIdx.push_back(l);
                ascs.push_back(dir ^ !ascendingOrder);
            }

            std::vector<int64_t> zs;
            int mw = getColWidth(orderField);
            zs = BoolLessBatchOperator(&xs, &ys, mw, 0, msgTagOffset,
                                       SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

            size_t comparingCount = xs.size();
            for (int i = 0; i < comparingCount; i++) {
                if (!ascs[i]) {
                    zs[i] = zs[i] ^ Comm::rank();
                }
            }

            size_t sz = comparingCount * (_colNum - 1);
            xs.reserve(sz);
            ys.reserve(sz);
            // xs and ys has already stored one order column
            int ofi = 0; // Order field index
            for (int i = 0; i < _colNum - 1; i++) {
                if (_fieldNames[i] == orderField) {
                    ofi = i;
                    continue;
                }
                for (int m = 0; m < comparingCount; m++) {
                    xs.push_back(_dataCols[i][xIdx[m]]);
                    ys.push_back(_dataCols[i][yIdx[m]]);
                }
            }

            zs = BoolMutexBatchOperator(&xs, &ys, &zs, _maxWidth, 0, msgTagOffset).
                    execute()->_zis;

            auto &oc = _dataCols[ofi]; // order column
            for (int i = 0; i < comparingCount; i++) {
                oc[xIdx[i]] = zs[i];
                oc[yIdx[i]] = zs[(_colNum - 1) * comparingCount + i];
            }

            // skip first for order column
            int pos = 1;
            for (int i = 0; i < _colNum - 1; i++) {
                if (i == ofi) {
                    continue;
                }
                auto &co = _dataCols[i];
                for (int m = 0; m < comparingCount; m++) {
                    co[xIdx[m]] = zs[pos * comparingCount + m];
                    co[yIdx[m]] = zs[(pos + _colNum - 1) * comparingCount + m];
                }
                pos++;
            }
        }
    }
}

void View::bsNB(const std::string &orderField, bool ascendingOrder, int msgTagOffset) {
    const int batchSize = Conf::BATCH_SIZE;
    const int n = static_cast<int>(_dataCols[0].size());
    auto &orderCol = getColData(orderField);
    const int mw = getColWidth(orderField);
    auto &paddings = _dataCols[_colNum + PADDING_COL_OFFSET];
    for (int k = 2; k <= n; k <<= 1) {
        for (int j = k >> 1; j > 0; j >>= 1) {
            int comparingCount = 0;
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
                    continue;
                }
                if (paddings[i] || paddings[l]) {
                    continue;
                }
                ++comparingCount;
            }
            if (comparingCount == 0) {
                continue;
            }
            std::vector<int> xIdx(comparingCount), yIdx(comparingCount);
            std::vector<bool> dirs(comparingCount);
            int pos = 0;
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
                    continue;
                }
                if (paddings[i] || paddings[l]) {
                    continue;
                }
                xIdx[pos] = i;
                yIdx[pos] = l;
                dirs[pos] = dir;
                ++pos;
            }
            const int numBatches = (comparingCount + batchSize - 1) / batchSize;
            const int tagStride = BoolMutexBatchOperator::msgTagCount() * (_colNum - 1);
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
                    std::vector<bool> ascs;
                    ascs.reserve(cnt);
                    for (int t = start; t < end; ++t) {
                        xs.push_back(orderCol[xIdx[t]]);
                        ys.push_back(orderCol[yIdx[t]]);
                        ascs.push_back(dirs[t] ^ !ascendingOrder);
                    }
                    auto zs = BoolLessBatchOperator(&xs, &ys, mw, 0,
                                                    msgTagOffset + tagStride * b,
                                                    SecureOperator::NO_CLIENT_COMPUTE).execute()->
                            _zis;
                    for (int t = 0; t < cnt; ++t) {
                        if (!ascs[t]) {
                            zs[t] ^= Comm::rank();
                        }
                    }

                    std::vector<std::future<void> > futures2;
                    futures2.reserve(_colNum - 1);
                    for (int c = 0; c < _colNum - 1; ++c) {
                        futures2.push_back(ThreadPoolSupport::submit([&, c] {
                            std::vector<int64_t> subXs, subYs;
                            if (_fieldNames[c] == orderField) {
                                subXs = std::move(xs);
                                subYs = std::move(ys);
                            } else {
                                subXs.reserve(comparingCount);
                                subYs.reserve(comparingCount);
                                for (int t = start; t < end; ++t) {
                                    subXs.push_back(_dataCols[c][xIdx[t]]);
                                    subYs.push_back(_dataCols[c][yIdx[t]]);
                                }
                            }

                            auto zs2 = BoolMutexBatchOperator(&subXs, &subYs, &zs, _maxWidth, 0,
                                                              msgTagOffset + tagStride * b +
                                                              BoolMutexBatchOperator::msgTagCount() * c).execute()->
                                    _zis;

                            auto &col = _dataCols[c];
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

View View::selectAll(Table &t) {
    View v(t._tableName, t._fieldNames, t._fieldWidths);
    v._dataCols = t._dataCols;
    v._fieldNames.emplace_back("$valid");
    v._dataCols.emplace_back(v._dataCols[0].size());
    v._fieldNames.emplace_back("$padding");
    v._dataCols.emplace_back(v._dataCols[0].size());
    return v;
}

View View::selectColumns(Table &t, std::vector<std::string> &fieldNames) {
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

    v._dataCols.reserve(indices.size());
    for (auto idx: indices) {
        v._dataCols.push_back(t._dataCols[idx]);
    }
    v._fieldNames.emplace_back("$valid");
    v._dataCols.emplace_back(v._dataCols[0].size());
    v._fieldNames.emplace_back("$padding");
    v._dataCols.emplace_back(v._dataCols[0].size());

    return v;
}

void View::bitonicSort(const std::string &orderField, bool ascendingOrder, int msgTagOffset) {
    if (Conf::BATCH_SIZE <= 0 || Conf::DISABLE_MULTI_THREAD) {
        bs1B(orderField, ascendingOrder, msgTagOffset);
    } else {
        bsNB(orderField, ascendingOrder, msgTagOffset);
    }
}
