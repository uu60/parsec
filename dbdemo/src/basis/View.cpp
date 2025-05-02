//
// Created by 杜建璋 on 25-4-25.
//

#include "../../include/basis/View.h"

#include "compute/batch/bool/BoolAndBatchOperator.h"
#include "compute/batch/bool/BoolLessBatchOperator.h"
#include "compute/batch/bool/BoolMutexBatchOperator.h"
#include "utils/Log.h"
#include "utils/StringUtils.h"

View::View(std::string &tableName, std::vector<std::string> &fieldNames, std::vector<int> &fieldWidths) : Table(
    tableName, fieldNames, fieldWidths) {
    _cols++;
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
    doSort(orderField, ascendingOrder, msgTagOffset);
    if (n < _dataCols[0].size()) {
        for (auto &v: _dataCols) {
            v.resize(n);
        }
    }
}

void View::collect(bool ascendingOrder, int n, const std::vector<int64_t> &col, int k, int j, std::vector<int64_t> &xs,
                   std::vector<int64_t> &ys, std::vector<int64_t> &xIdx, std::vector<int64_t> &yIdx,
                   std::vector<bool> &ascs) {
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
        auto &paddings = _dataCols[_cols - 1];
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
}

void View::compareAndSwap(const std::string &orderField, int msgTagOffset, std::vector<int64_t> &xs,
                          std::vector<int64_t> &ys, std::vector<int64_t> &xIdx, std::vector<int64_t> &yIdx,
                          std::vector<bool> &ascs, std::vector<int64_t> &zs) {
    int mw = getColWidth(orderField);
    zs = BoolLessBatchOperator(&xs, &ys, mw, _taskTag, msgTagOffset,
                               SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

    size_t comparingCount = xs.size();
    for (int i = 0; i < comparingCount; i++) {
        if (!ascs[i]) {
            zs[i] = zs[i] ^ Comm::rank();
        }
    }

    size_t sz = comparingCount * (_cols - 1);
    xs.reserve(sz);
    ys.reserve(sz);
    // xs and ys has already stored one order column
    int ofi = 0; // Order field index
    for (int i = 0; i < _cols - 1; i++) {
        if (_fieldNames[i] == orderField) {
            ofi = i;
            continue;
        }
        for (int j = 0; j < comparingCount; j++) {
            xs.push_back(_dataCols[i][xIdx[j]]);
            ys.push_back(_dataCols[i][yIdx[j]]);
        }
    }

    zs = BoolMutexBatchOperator(&xs, &ys, &zs, _maxColWidth, _taskTag, msgTagOffset).execute()->_zis;

    auto &oc = _dataCols[ofi]; // order column
    for (int j = 0; j < comparingCount; j++) {
        oc[xIdx[j]] = zs[j];
        oc[yIdx[j]] = zs[(_cols - 1) * comparingCount + j];
    }

    // skip first for order column
    int pos = 1;
    for (int i = 0; i < _cols - 1; i++) {
        if (i == ofi) {
            continue;
        }
        auto &col = _dataCols[i];
        for (int j = 0; j < comparingCount; j++) {
            col[xIdx[j]] = zs[pos * comparingCount + j];
            col[yIdx[j]] = zs[(pos + _cols - 1) * comparingCount + j];
        }
        pos++;
    }
}

void View::doSort(const std::string &orderField, bool ascendingOrder, int msgTagOffset) {
    int n = _dataCols[0].size();
    auto &col = getColData(orderField);
    for (int k = 2; k <= n; k *= 2) {
        for (int j = k / 2; j > 0; j /= 2) {
            std::vector<int64_t> xs;
            std::vector<int64_t> ys;
            std::vector<int64_t> xIdx;
            std::vector<int64_t> yIdx;
            std::vector<bool> ascs;
            collect(ascendingOrder, n, col, k, j, xs, ys, xIdx, yIdx, ascs);

            std::vector<int64_t> zs;
            compareAndSwap(orderField, msgTagOffset, xs, ys, xIdx, yIdx, ascs, zs);
        }
    }
}
