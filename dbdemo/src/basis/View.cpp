//
// Created by 杜建璋 on 25-4-25.
//

#include "../../include/basis/View.h"

#include "compute/batch/bool/BoolAndBatchOperator.h"
#include "compute/batch/bool/BoolLessBatchOperator.h"
#include "compute/batch/bool/BoolMutexBatchOperator.h"
#include "parallel/ThreadPoolSupport.h"
#include "utils/Log.h"
#include "utils/StringUtils.h"

View::View(std::string &tableName,
           std::vector<std::string> &fieldNames,
           std::vector<int> &fieldWidths) : Table(
    tableName, fieldNames, fieldWidths) {
    // for padding column
    _colNum++;
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
                auto &paddings = _dataCols[_colNum - 1];
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
    auto &paddings = _dataCols[_colNum - 1];
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
    v._fieldNames.emplace_back("$padding");
    v._dataCols.emplace_back(v._dataCols[0].size());
    return v;
}

View View::selectColumns(Table &t, std::vector<std::string> &fieldNames) {
    std::vector<size_t> indices;
    indices.reserve(fieldNames.size());
    for (const auto &name : fieldNames) {
        auto it = std::find(t._fieldNames.begin(), t._fieldNames.end(), name);
        indices.push_back(std::distance(t._fieldNames.begin(), it));
    }

    std::vector<int> widths;
    widths.reserve(indices.size());
    for (auto idx : indices) {
        widths.push_back(t._fieldWidths[idx]);
    }

    View v(t._tableName, fieldNames, widths);

    if (t._dataCols.empty()) {
        return v;
    }

    v._dataCols.reserve(indices.size());
    for (auto idx : indices) {
        v._dataCols.push_back(t._dataCols[idx]);
    }
    // padding col
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
