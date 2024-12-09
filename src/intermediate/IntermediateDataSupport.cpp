//
// Created by 杜建璋 on 2024/11/25.
//

#include "intermediate//IntermediateDataSupport.h"

#include "comm/IComm.h"
#include "intermediate//ABPairGenerator.h"
#include "intermediate//OtBmtGenerator.h"

BlockingQueue<Bmt> IntermediateDataSupport::_bmts(10000);
BlockingQueue<ABPair> IntermediateDataSupport::_pairs(10000);

void IntermediateDataSupport::offerBmt(Bmt bmt) {
    _bmts.push(bmt);
}

void IntermediateDataSupport::offerABPair(ABPair pair) {
    _pairs.push(pair);
}

std::vector<Bmt> IntermediateDataSupport::pollBmts(int num) {
    std::vector<Bmt> ret;
    for (int i = 0; i < num; i++) {
        ret.push_back(_bmts.pop());
    }
    return ret;
}

std::vector<ABPair> IntermediateDataSupport::pollABPairs(int num) {
    std::vector<ABPair> ret;
    for (int i = 0; i < num; i++) {
        ret.push_back(_pairs.pop());
    }
    return ret;
}

void IntermediateDataSupport::startGenerateBmtsAsync() {
    if (IComm::impl->isServer()) {
        System::_threadPool.push([] (int _) {
            while (!System::_shutdown.load()) {
                offerBmt(OtBmtGenerator::getInstance().execute()->_bmt);
            }
        });
    }
}

void IntermediateDataSupport::startGenerateABPairsAsyc() {
    if (IComm::impl->isServer()) {
        System::_threadPool.push([] (int _) {
            while (!System::_shutdown.load()) {
                offerABPair(ABPairGenerator::getInstance().execute()->_pair);
            }
        });
    }
}
