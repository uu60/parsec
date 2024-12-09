//
// Created by 杜建璋 on 2024/11/25.
//

#ifndef BMTHOLDER_H
#define BMTHOLDER_H
#include <vector>

#include "./ABPair.h"
#include "./Bmt.h"
#include "../sync/BlockingQueue.h"


class IntermediateDataSupport {
private:
    static BlockingQueue<Bmt> _bmts;
    static BlockingQueue<ABPair> _pairs;

public:
    static void offerBmt(Bmt bmt);

    static void offerABPair(ABPair pair);

    static std::vector<Bmt> pollBmts(int num);

    static std::vector<ABPair> pollABPairs(int num);

    /**
     * Loop forever to insert bmts into queue.
     * ASYNCHRONOUSLY
     */
    static void startGenerateBmtsAsync();

    static void startGenerateABPairsAsyc();
};


#endif //BMTHOLDER_H
