//
// Created by 杜建璋 on 2025/2/24.
//

#include "compute/batch/bool/BoolBatchOperator.h"

#include "conf/Conf.h"
#include "parallel/ThreadPoolSupport.h"
#include "utils/Math.h"

BoolBatchOperator::BoolBatchOperator(std::vector<int64_t> &zs, int width, int taskTag, int msgTagOffset,
                                     int clientRank) : AbstractBatchOperator(width, taskTag, msgTagOffset) {
    if (clientRank < 0) {
        _zis = zs;
    } else {
        // distribute operator
        if (Comm::rank() == clientRank) {
            std::vector<int64_t> zv0, zv1;
            zv0.reserve(zs.size());
            zv1.reserve(zs.size());
            for (auto z: zs) {
                int64_t z1 = ring(Math::randInt());
                int64_t z0 = ring(z ^ z1);
                zv0.push_back(z0);
                zv1.push_back(z1);
            }
            auto r = Comm::sendAsync(zv0, _width, 0, buildTag(_currentMsgTag));
            Comm::send(zv1, _width, 1, buildTag(_currentMsgTag));
            Comm::wait(r);
        } else if (Comm::isServer()) {
            _zis.clear();
            // operator
            Comm::receive(_zis, _width, clientRank, buildTag(_currentMsgTag));
        }
    }
}

BoolBatchOperator::BoolBatchOperator(std::vector<int64_t> *xs, std::vector<int64_t> *ys, int width, int taskTag,
                                     int msgTagOffset, int clientRank) : AbstractBatchOperator(
    width, taskTag, msgTagOffset) {
    if (clientRank < 0) {
        _xis = xs;
        _yis = ys;
    } else {
        // distribute operator
        if (Comm::rank() == clientRank) {
            std::vector<int64_t> v0, v1;
            size_t size = xs->size();
            v0.reserve(2 * size);
            v1.reserve(2 * size);

            for (auto x: *xs) {
                int64_t x1 = ring(Math::randInt());
                int64_t x0 = ring(x ^ x1);
                v0.push_back(x0);
                v1.push_back(x1);
            }
            for (auto y: *ys) {
                int64_t y1 = ring(Math::randInt());
                int64_t y0 = ring(y ^ y1);
                v0.push_back(y0);
                v1.push_back(y1);
            }
            auto r = Comm::sendAsync(v0, _width, 0, buildTag(_currentMsgTag));
            Comm::send(v1, _width, 1, buildTag(_currentMsgTag));
            Comm::wait(r);
        } else if (Comm::isServer()) {
            // operator
            std::vector<int64_t> temp;
            Comm::receive(temp, _width, clientRank, buildTag(_currentMsgTag));
            size_t size = temp.size() / 2;
            _xis = new std::vector<int64_t>(size);
            _yis = new std::vector<int64_t>(size);
            for (int i = 0; i < size; i++) {
                (*_xis)[i] = temp[i];
                (*_yis)[i] = temp[i + size];
            }
        }
    }
}

BoolBatchOperator *BoolBatchOperator::reconstruct(int clientRank) {
    _currentMsgTag = _startMsgTag;
    if (Comm::isServer()) {
        Comm::send(_zis, _width, clientRank, buildTag(_currentMsgTag));
    } else if (Comm::rank() == clientRank) {
        std::vector<int64_t> temp0, temp1;
        Comm::receive(temp0, _width, 0, buildTag(_currentMsgTag));
        Comm::receive(temp1, _width, 1, buildTag(_currentMsgTag));
        _results.clear();
        _results.reserve(temp0.size() + temp1.size());
        for (int i = 0; i < temp0.size(); i++) {
            _results.push_back(ring(temp0[i] ^ temp1[i]));
        }
    }
    return this;
}

BoolBatchOperator *BoolBatchOperator::execute() {
    throw std::runtime_error("Needs implementation.");
}
