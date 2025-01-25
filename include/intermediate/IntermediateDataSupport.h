//
// Created by 杜建璋 on 2024/11/25.
//

#ifndef BMTHOLDER_H
#define BMTHOLDER_H

#include <vector>

#include "./item/ABPair.h"
#include "./item/Bmt.h"
#include "./item/SRot.h"
#include "../sync/LockBlockingQueue.h"
#include "./item/RRot.h"
#include "../sync/CasBlockingQueue.h"
#include "../conf/Conf.h"


class IntermediateDataSupport {
private:
    inline static BlockingQueue<Bmt> *_bmts = Conf::BMT_QUEUE;
    // static LockBlockingQueue<Bmt> _bmts;
    // static LockBlockingQueue<ABPair> _pairs;
    inline static Bmt *currentBmt{};
    inline static int currentBmtLeftTimes = Conf::BMT_USAGE_LIMIT;
    inline static std::atomic_bool generatingBmts{false};

public:
    inline static SRot *_sRot = nullptr;
    inline static RRot *_rRot = nullptr;

public:
    static void offerBmt(Bmt bmt);

    // static void offerABPair(ABPair pair);

    static void prepareRot();

    static std::vector<Bmt> pollBmts(int count, int l);

    // static std::vector<ABPair> pollABPairs(int num);

    /**
     * Loop forever to insert bmts into queue.
     * ASYNCHRONOUSLY
     */
    static void startGenerateBmtsAsync();

    // static void startGenerateABPairsAsyc();
};


#endif //BMTHOLDER_H
