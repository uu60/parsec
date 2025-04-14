//
// Created by 杜建璋 on 2024/11/25.
//

#ifndef BMTHOLDER_H
#define BMTHOLDER_H

#include <vector>

#include "./item/Bmt.h"
#include "./item/SRot.h"
#include "./item/RRot.h"
#include "../conf/Conf.h"
#include "./item/BitwiseBmt.h"
#include "sync/AbstractBlockingQueue.h"


class IntermediateDataSupport {
private:
    // inline static AbstractBlockingQueue<Bmt> *_bmts;
    // inline static AbstractBlockingQueue<BitwiseBmt> *_bitwiseBmts;
    inline static std::vector<AbstractBlockingQueue<Bmt> *> _bmtQs;
    inline static std::vector<AbstractBlockingQueue<BitwiseBmt> *> _bitwiseBmtQs;
    inline static u_int _currentBmtQ = 0;
    inline static u_int _currentBitwiseBmtQ = 0;

    inline static Bmt *_currentBmt{};
    inline static BitwiseBmt *_currentBitwiseBmt{};

    inline static int _currentBmtLeftTimes = Conf::BMT_USAGE_LIMIT;
    inline static int _currentBitwiseBmtLeftTimes = Conf::BMT_USAGE_LIMIT;

public:
    inline static Bmt _fixedBmt;
    inline static BitwiseBmt _fixedBitwiseBmt;

    inline static SRot *_sRot = nullptr;
    inline static RRot *_rRot = nullptr;

private:
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

    static void finalize();

    // MUST SEQUENTIAL ACCESS
    static std::vector<Bmt> pollBmts(int count, int width);

    // MUST SEQUENTIAL ACCESS
    static std::vector<BitwiseBmt> pollBitwiseBmts(int count, int width);
};


#endif //BMTHOLDER_H
