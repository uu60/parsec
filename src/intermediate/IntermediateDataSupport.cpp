//
// Created by 杜建璋 on 2024/11/25.
//

#include "intermediate//IntermediateDataSupport.h"

#include "comm/IComm.h"
#include "intermediate//ABPairGenerator.h"
#include "intermediate//OtBmtGenerator.h"

folly::MPMCQueue<Bmt> IntermediateDataSupport::_bmts(10000);
folly::MPMCQueue<ABPair> IntermediateDataSupport::_pairs(10000);

void IntermediateDataSupport::offerBmt(Bmt bmt) {
    _bmts.blockingWrite(bmt);
}

void IntermediateDataSupport::offerABPair(ABPair pair) {
    _pairs.blockingWrite(pair);
}

std::vector<Bmt> IntermediateDataSupport::pollBmts(int num) {
    std::vector<Bmt> ret;
    for (int i = 0; i < num; i++) {
        Bmt b;
        _bmts.blockingRead(b);
        ret.push_back(b);
    }
    return ret;
}

std::vector<ABPair> IntermediateDataSupport::pollABPairs(int num) {
    std::vector<ABPair> ret;
    for (int i = 0; i < num; i++) {
        ABPair b{};
        _pairs.blockingRead(b);
        ret.push_back(b);
    }
    return ret;
}

void IntermediateDataSupport::startGenerateBmtsAsync() {
    if (IComm::impl->isServer()) {
        System::_threadPool.add([] {
            for (;;) {
                offerBmt(OtBmtGenerator::getInstance().execute()->_bmt);
            }
        });
    }
}

void IntermediateDataSupport::startGenerateABPairsAsyc() {
    if (IComm::impl->isServer()) {
        System::_threadPool.add([] {
            for (;;) {
                offerABPair(ABPairGenerator::getInstance().execute()->_pair);
            }
        });
    }
}
