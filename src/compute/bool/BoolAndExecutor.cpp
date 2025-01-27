//
// Created by 杜建璋 on 2024/11/12.
//

#include "compute/bool/BoolAndExecutor.h"

#include "compute/arith/ArithMultiplyExecutor.h"
#include "comm/Comm.h"
#include "intermediate/BmtGenerator.h"
#include "intermediate/IntermediateDataSupport.h"
#include "utils/Log.h"
#include "utils/Math.h"

BoolAndExecutor *BoolAndExecutor::execute() {
    _currentMsgTag = _startMsgTag;
    if (Comm::isServer()) {
        int64_t ei = 0, fi = 0;
        std::vector<Bmt> bmts;
        bmts.reserve(_l);
        if (_bmts != nullptr) {
            bmts = *_bmts;
        } else if (Conf::INTERM_PREGENERATED) {
            bmts = IntermediateDataSupport::pollBmts(_l, 1);
        } else {
            int ul = Conf::BMT_USAGE_LIMIT;
            std::vector<std::future<Bmt> > futures;
            // e.g. If _l == 64, ul == 10, we need ceil(64 / 10) = 7 bmts.
            int needed = std::ceil(static_cast<double>(_l) / ul);
            futures.reserve(needed);
            for (int i = 0; i < needed; i++) {
                futures.push_back(System::_threadPool.push([&, i](int) {
                    return BmtGenerator(1, _taskTag,
                                        static_cast<int16_t>(_currentMsgTag + i * BmtGenerator::needMsgTags(1))).
                            execute()->_bmt;
                }));
            }
            for (int i = 0; i < _l; i++) {
                if (i % ul == 0) {
                    bmts.emplace_back(futures[i / ul].get());
                } else {
                    bmts.emplace_back(bmts[i - 1]);
                }
            }
        }

        for (int i = 0; i < _l; i++) {
            ei = Math::changeBit(ei, i, Math::getBit(_xi, i) ^ bmts[i]._a);
            fi = Math::changeBit(fi, i, Math::getBit(_yi, i) ^ bmts[i]._b);
            bmts.emplace_back(bmts[i]);
        }

        std::vector efi = {ei, fi};
        std::vector<int64_t> efo;
        Comm::serverSend(efi, buildTag(_currentMsgTag));
        Comm::serverReceive(efo, buildTag(_currentMsgTag));
        int64_t e = ring(ei ^ efo[0]);
        int64_t f = ring(fi ^ efo[1]);
        for (int i = 0; i < _l; i++) {
            bool e_bit_i = Math::getBit(e, i);
            bool f_bit_i = Math::getBit(f, i);
            _zi = Math::changeBit(
                _zi, i,
                (Comm::rank() & e_bit_i & f_bit_i) ^ (f_bit_i & bmts[i]._a) ^ (e_bit_i & bmts[i]._b) ^ bmts[i]._c);
        }
    }
    return this;
}

std::string BoolAndExecutor::className() const {
    return "BoolAndExecutor";
}

int16_t BoolAndExecutor::needMsgTags(int l) {
    return static_cast<int16_t>(std::ceil(static_cast<double>(l) / Conf::BMT_USAGE_LIMIT) * BmtGenerator::needMsgTags(l));
}

std::pair<int, int> BoolAndExecutor::needBmtsWithBits(int l) {
    return {l, 1};
}

BoolAndExecutor *BoolAndExecutor::setBmts(std::vector<Bmt> *bmts) {
    _bmts = bmts;
    return this;
}
