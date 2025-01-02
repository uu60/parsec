//
// Created by 杜建璋 on 2024/11/25.
//

#include "intermediate//IntermediateDataSupport.h"

#include "comm/IComm.h"
#include "intermediate/ABPairGenerator.h"
#include "intermediate/BmtGenerator.h"
#include "ot/BaseOtExecutor.h"
#include "utils/Log.h"
#include "utils/Math.h"

BlockingQueue<Bmt> IntermediateDataSupport::_bmts(100000);
BlockingQueue<ABPair> IntermediateDataSupport::_pairs(100000);

void IntermediateDataSupport::offerBmt(Bmt bmt) {
    _bmts.push(bmt);
}

void IntermediateDataSupport::offerABPair(ABPair pair) {
    _pairs.push(pair);
}

void IntermediateDataSupport::prepareRot() {
    if (IComm::impl->isServer()) {
        std::vector<std::future<void> > futures;
        futures.reserve(2);

        for (int i = 0; i < 2; i++) {
            futures.push_back(System::_threadPool.push([i](int _) {
                bool isSender = IComm::impl->rank() == i;
                if (isSender) {
                    _sRot = new SRot();
                    _sRot->_r0 = Math::randInt();
                    _sRot->_r1 = Math::randInt();
                } else {
                    _rRot = new RRot();
                    _rRot->_b = static_cast<int>(Math::randInt(0, 1));;
                }
                BaseOtExecutor e(i, isSender ? _sRot->_r0 : -1, isSender ? _sRot->_r1 : -1, !isSender ? _rRot->_b : -1,
                                 64, 2, i * BaseOtExecutor::needsMsgTags());
                e.execute();
                if (!isSender) {
                    _rRot->_rb = e._result;
                }
            }));
        }

        for (auto &f: futures) {
            f.wait();
        }
    }
}

std::vector<Bmt> IntermediateDataSupport::pollBmts(int num) {
    std::vector<Bmt> ret;
    if (IComm::impl->isServer()) {
        ret.reserve(num);
        for (int i = 0; i < num; i++) {
            _bmts.pop();
            ret.push_back(_bmts.pop());
        }
    }
    return ret;
}

std::vector<ABPair> IntermediateDataSupport::pollABPairs(int num) {
    std::vector<ABPair> ret;
    if (IComm::impl->isServer()) {
        ret.reserve(num);
        for (int i = 0; i < num; i++) {
            ret.push_back(_pairs.pop());
        }
    }
    return ret;
}

void IntermediateDataSupport::startGenerateBmtsAsync() {
    if (IComm::impl->isServer()) {
        System::_threadPool.push([](int _) {
            while (!System::_shutdown.load()) {
                offerBmt(BmtGenerator::getInstance().execute()->_bmt);
            }
        });
    }
}

void IntermediateDataSupport::startGenerateABPairsAsyc() {
    if (IComm::impl->isServer()) {
        System::_threadPool.push([](int _) {
            while (!System::_shutdown.load()) {
                offerABPair(ABPairGenerator::getInstance().execute()->_pair);
            }
        });
    }
}
