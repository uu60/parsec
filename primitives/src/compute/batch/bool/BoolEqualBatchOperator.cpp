#include "compute/batch/bool/BoolEqualBatchOperator.h"

#include "compute/batch/bool/BoolAndBatchOperator.h"
#include "compute/batch/bool/BoolXorBatchOperator.h"

#include "utils/Math.h"

namespace parsec {

BoolEqualBatchOperator* BoolEqualBatchOperator::execute() {
    if (Comm::isClient()) {
        return this;
    }

    std::vector<BitwiseBmt> allBmts;
    bool gotBmt = prepareBmts(allBmts);

    auto diff = BoolXorBatchOperator(_xis, _yis, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE).execute()->_zis;

    std::vector<int64_t> values;
    values.reserve(diff.size() * _width);
    for (auto v : diff) {
        auto equalBits = Math::ring(Comm::rank() == 0 ? ~v : v, _width);
        for (int bit = 0; bit < _width; ++bit) {
            values.push_back(Math::getBit(equalBits, bit));
        }
    }

    int activeWidth = _width;
    size_t bmtOffset = 0;

    while (activeWidth > 1) {
        const int pairCountPerRecord = activeWidth / 2;
        const int nextWidth = (activeWidth + 1) / 2;
        const int recordCount = static_cast<int>(diff.size());

        std::vector<int64_t> lhs;
        std::vector<int64_t> rhs;
        lhs.reserve(recordCount * pairCountPerRecord);
        rhs.reserve(recordCount * pairCountPerRecord);

        std::vector<int64_t> carry(recordCount, 0);
        for (int row = 0; row < recordCount; ++row) {
            auto base = static_cast<size_t>(row * activeWidth);
            for (int bit = 0; bit + 1 < activeWidth; bit += 2) {
                lhs.push_back(values[base + bit]);
                rhs.push_back(values[base + bit + 1]);
            }
            if (activeWidth % 2 == 1) {
                carry[row] = values[base + activeWidth - 1];
            }
        }

        std::vector<int64_t> andResult;
        if (!lhs.empty()) {
            std::vector<BitwiseBmt> roundBmts;
            if (gotBmt) {
                auto roundBmtCount = BoolAndBatchOperator::bmtCount(static_cast<int>(lhs.size()), 1);
                roundBmts.assign(allBmts.begin() + static_cast<long>(bmtOffset),
                                 allBmts.begin() + static_cast<long>(bmtOffset + roundBmtCount));
                bmtOffset += roundBmtCount;
            }
            andResult = BoolAndBatchOperator(&lhs, &rhs, 1, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE)
                            .setBmts(gotBmt ? &roundBmts : nullptr)
                            ->execute()
                            ->_zis;
        }

        std::vector<int64_t> next;
        next.reserve(recordCount * nextWidth);
        size_t andIndex = 0;
        for (int row = 0; row < recordCount; ++row) {
            for (int j = 0; j < pairCountPerRecord; ++j) {
                next.push_back(andResult[andIndex++] & 1);
            }
            if (activeWidth % 2 == 1) {
                next.push_back(carry[row] & 1);
            }
        }

        values = std::move(next);
        activeWidth = nextWidth;
    }

    if (gotBmt && bmtOffset != allBmts.size()) {
        throw std::runtime_error("BoolEqualBatchOperator did not consume the expected number of BMTs");
    }
    _zis = std::move(values);
    return this;
}

BoolEqualBatchOperator* BoolEqualBatchOperator::setBmts(std::vector<BitwiseBmt>* bmts) {
    if (bmts != nullptr && bmts->size() != bmtCount(static_cast<int>(_xis->size()), _width)) {
        throw std::runtime_error("BoolEqualBatchOperator BMT count mismatch");
    }
    this->_bmts = bmts;
    return this;
}

int BoolEqualBatchOperator::tagStride() { return BoolAndBatchOperator::tagStride(); }

int BoolEqualBatchOperator::bmtCount(int num, int width) {
    int count = 0;
    int activeWidth = width;
    while (activeWidth > 1) {
        count += BoolAndBatchOperator::bmtCount(num * (activeWidth / 2), 1);
        activeWidth = (activeWidth + 1) / 2;
    }
    return count;
}

bool BoolEqualBatchOperator::prepareBmts(std::vector<BitwiseBmt>& bmts) {
    if (_bmts != nullptr) {
        bmts = std::move(*_bmts);
        return true;
    }
    return false;
}

} // namespace parsec
