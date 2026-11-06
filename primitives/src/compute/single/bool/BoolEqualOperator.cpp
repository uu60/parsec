
#include "compute/single/bool/BoolEqualOperator.h"

#include "comm/Comm.h"
#include "compute/single/bool/BoolAndOperator.h"
#include "intermediate/IntermediateDataSupport.h"
#include "utils/Math.h"

namespace parsec {

BoolEqualOperator* BoolEqualOperator::execute() {
    if (Comm::isServer()) {
        int64_t diff = ring(_xi ^ _yi);
        if (Comm::rank() == 0) {
            diff = ring(~diff);
        }

        std::vector<int64_t> values;
        values.reserve(_width);
        for (int i = 0; i < _width; ++i) {
            values.push_back(Math::getBit(diff, i));
        }

        int msgTag = _startMsgTag;
        bool consumedExternalBmt = false;
        while (values.size() > 1) {
            std::vector<int64_t> next;
            next.reserve((values.size() + 1) / 2);

            for (size_t i = 0; i + 1 < values.size(); i += 2) {
                BitwiseBmt bmt;
                BitwiseBmt* bmtPtr = nullptr;
                if (_bmt != nullptr && !consumedExternalBmt) {
                    bmt = *_bmt;
                    bmtPtr = &bmt;
                    consumedExternalBmt = true;
                } else if (Conf::BMT_METHOD == Conf::BMT_BACKGROUND || Conf::BMT_METHOD == Conf::BMT_PIPELINE) {
                    bmt = IntermediateDataSupport::pollBitwiseBmts(1, 1)[0];
                    bmtPtr = &bmt;
                }

                auto zi = BoolAndOperator(values[i], values[i + 1], 1, _taskTag, msgTag, NO_CLIENT_COMPUTE)
                              .setBmt(bmtPtr)
                              ->execute()
                              ->_zi;
                next.push_back(zi & 1);
                msgTag += BoolAndOperator::tagStride(1);
            }

            if (values.size() % 2 == 1) {
                next.push_back(values.back() & 1);
            }
            values = std::move(next);
        }

        _zi = values.empty() ? 0 : (values[0] & 1);
    }
    return this;
}

BoolEqualOperator* BoolEqualOperator::setBmt(BitwiseBmt* bmt) {
    _bmt = bmt;
    return this;
}

} // namespace parsec
