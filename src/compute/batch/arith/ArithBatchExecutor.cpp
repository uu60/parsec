//
// Created by 杜建璋 on 2025/1/30.
//

#include "compute/batch/arith/ArithBatchExecutor.h"

#include "conf/Conf.h"
#include "parallel/ThreadPoolSupport.h"
#include "utils/Math.h"

ArithBatchExecutor::ArithBatchExecutor(std::vector<int64_t>& zs, int width, int taskTag, int msgTagOffset,
                                       int clientRank) : AbstractBatchExecutor(width, taskTag, msgTagOffset) {
    if (clientRank < 0) {
        _zis = std::move(zs);
    } else {
        // distribute operator
        if (Comm::rank() == clientRank) {
            std::vector<int64_t> zv0, zv1;
            zv0.reserve(zs.size());
            zv1.reserve(zs.size());
            for (auto z : zs) {
                int64_t z1 = Math::randInt();
                int64_t z0 = z - z1;
                zv0.push_back(z0);
                zv1.push_back(z1);
            }
            std::future<void> f;
            if constexpr (Conf::INTRA_OPERATOR_PARALLELISM) {
                // To avoid long time waiting on network, send data in parallel.
                f = ThreadPoolSupport::submit([&] {
                    Comm::send(zv0, _width, 0, buildTag(_currentMsgTag));
                });
            } else {
                Comm::send(zv0, _width, 0, buildTag(_currentMsgTag));
            }
            Comm::send(zv1, _width, 1, buildTag(_currentMsgTag));
            if constexpr (Conf::INTRA_OPERATOR_PARALLELISM) {
                f.wait();
            }
        } else if (Comm::isServer()) {
            _zis.clear();
            // operator
            Comm::receive(_zis, _width, clientRank, buildTag(_currentMsgTag));
        }
    }
}

ArithBatchExecutor::ArithBatchExecutor(std::vector<int64_t> &xs, std::vector<int64_t> &ys, int width, int taskTag,
    int msgTagOffset, int clientRank) : AbstractBatchExecutor(width, taskTag, msgTagOffset) {
    if (clientRank < 0) {
        _xis = std::move(xs);
        _yis = std::move(ys);
    } else {
        // distribute operator
        if (Comm::rank() == clientRank) {
            std::vector<int64_t> v0, v1;
            size_t size = xs.size();
            v0.reserve(2 * size);
            v1.reserve(2 * size);

            for (auto x : xs) {
                int64_t x1 = Math::randInt();
                int64_t x0 = x - x1;
                v0.push_back(x0);
                v1.push_back(x1);
            }
            for (auto y : ys) {
                int64_t y1 = Math::randInt();
                int64_t y0 = y - y1;
                v0.push_back(y0);
                v1.push_back(y1);
            }
            std::future<void> f;
            if constexpr (Conf::INTRA_OPERATOR_PARALLELISM) {
                f = ThreadPoolSupport::submit([&] {
                    Comm::send(v0, _width, 0, buildTag(_currentMsgTag));
                });
            } else {
                Comm::send(v0, _width, 0, buildTag(_currentMsgTag));
            }
            Comm::send(v1, _width, 1, buildTag(_currentMsgTag));
            if constexpr (Conf::INTRA_OPERATOR_PARALLELISM) {
                // sync
                f.wait();
            }
        } else if (Comm::isServer()) {
            // operator
            std::vector<int64_t> temp;
            Comm::receive(temp, _width, clientRank, buildTag(_currentMsgTag));
            size_t size = temp.size() / 2;
            _xis.clear();
            _yis.clear();
            _xis.reserve(size);
            _yis.reserve(size);
            for (int i = 0; i < size * 2; i++) {
                if (i < size) {
                    _xis.push_back(temp[i]);
                } else {
                    _yis.push_back(temp[i]);
                }
            }
        }
    }
}

ArithBatchExecutor * ArithBatchExecutor::reconstruct(int clientRank) {
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
            _results.push_back(temp0[i] + temp1[i]);
        }
    }
    return this;
}

ArithBatchExecutor * ArithBatchExecutor::execute() {
    throw std::runtime_error("Needs implementation.");
}
