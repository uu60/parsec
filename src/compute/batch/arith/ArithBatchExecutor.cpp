//
// Created by 杜建璋 on 2025/1/30.
//

#include "compute/batch/arith/ArithBatchExecutor.h"

#include "conf/Conf.h"
#include "utils/Math.h"

ArithBatchExecutor::ArithBatchExecutor(std::vector<int64_t>& zs, int width, int16_t taskTag, int16_t msgTagOffset,
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
            if (Conf::INTRA_OPERATOR_PARALLELISM) {
                // To avoid long time waiting on network, send data in parallel.
                f = System::_threadPool.push([&](int) {
                    Comm::send(zv0, _width, 0, buildTag(_currentMsgTag));
                });
            } else {
                Comm::send(zv0, _width, 0, buildTag(_currentMsgTag));
            }
            Comm::send(zv1, _width, 1, buildTag(_currentMsgTag));
            if (Conf::INTRA_OPERATOR_PARALLELISM) {
                f.wait();
            }
        } else if (Comm::isServer()) {
            _zis.clear();
            // operator
            Comm::receive(_zis, _width, clientRank, buildTag(_currentMsgTag));
        }
    }
}

ArithBatchExecutor::ArithBatchExecutor(std::vector<int64_t> &x, std::vector<int64_t> &y, int width, int16_t taskTag,
    int16_t msgTagOffset, int clientRank) : AbstractBatchExecutor(width, taskTag, msgTagOffset) {

}

ArithBatchExecutor * ArithBatchExecutor::reconstruct(int clientRank) {
    return this;
}

ArithBatchExecutor * ArithBatchExecutor::execute() {
    return this;
}
