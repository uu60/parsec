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
#include "../sync/BoostLockFreeQueue.h"
#include "../conf/Conf.h"
#include "./item/BitwiseBmt.h"


class IntermediateDataSupport {
private:
    inline static AbstractBlockingQueue<Bmt> *_bmts;
    inline static AbstractBlockingQueue<BitwiseBmt> *_bitwiseBmts;
    // static LockBlockingQueue<Bmt> _bmts;
    // static LockBlockingQueue<ABPair> _pairs;
    inline static Bmt *_currentBmt{};
    inline static BitwiseBmt *_currentBitwiseBmt{};
    inline static int _currentBmtLeftTimes = Conf::BMT_USAGE_LIMIT;
    inline static int _currentBitwiseBmtLeftTimes = Conf::BMT_USAGE_LIMIT;

public:
    inline static Bmt _fixedBmt;
    inline static BitwiseBmt _fixedBitwiseBmt;

public:
    inline static SRot *_sRot = nullptr;
    inline static RRot *_rRot = nullptr;

private:
    static void offerBmt(Bmt bmt);

    static void offerBitwiseBmt(BitwiseBmt bmt);

    static void prepareBmt();

    static void prepareRot();

    /**
      * Loop forever to insert bmts into queue.
      * ASYNCHRONOUSLY
    */
    static void startGenerateBmtsAsync();

    static void startGenerateBitwiseBmtsAsync();

public:
    static void init();

    // static void offerABPair(ABPair pair);

    static std::vector<Bmt> pollBmts(int count, int width);

    static std::vector<BitwiseBmt> pollBitwiseBmts(int count, int width);

    // static std::vector<ABPair> pollABPairs(int num);

    // static void startGenerateABPairsAsyc();
};


#endif //BMTHOLDER_H
