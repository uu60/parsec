#ifndef BMTHOLDER_H
#define BMTHOLDER_H

#include <vector>
#include <array>
#include <cstdint>

#include "./item/Bmt.h"
#include "./item/SRot.h"
#include "./item/RRot.h"
#include "../conf/Conf.h"
#include "./item/BitwiseBmt.h"
#include "sync/AbstractBlockingQueue.h"


class IntermediateDataSupport {
private:
    inline static u_int _currentBmtQ = 0;
    inline static u_int _currentBitwiseBmtQ = 0;

    inline static Bmt *_currentBmt{};
    inline static BitwiseBmt *_currentBitwiseBmt{};
    inline static int _currentBmtLeftTimes = Conf::BMT_USAGE_LIMIT;
    inline static int _currentBitwiseBmtLeftTimes = Conf::BMT_USAGE_LIMIT;

public:
    inline static std::vector<AbstractBlockingQueue<Bmt> *> _bmtQs;
    inline static std::vector<AbstractBlockingQueue<BitwiseBmt> *> _bitwiseBmtQs;

    inline static Bmt _fixedBmt;
    inline static BitwiseBmt _fixedBitwiseBmt;

    inline static SRot *_sRot0 = nullptr;
    inline static RRot *_rRot0 = nullptr;
    inline static SRot *_sRot1 = nullptr;
    inline static RRot *_rRot1 = nullptr;

    // IKNP base seeds derived from base OT; computed once in init().
    // _iknpBaseSeeds[dir][i] where dir=0 means sender=0, dir=1 means sender=1
    // For each direction, the IKNP sender has one seed per row, receiver has two
    inline static std::vector<std::array<int64_t, 2>> _iknpBaseSeeds0;  // For sender=0 direction
    inline static std::vector<std::array<int64_t, 2>> _iknpBaseSeeds1;  // For sender=1 direction

    // IKNP sender's choice bits (128 bits, one per base OT)
    inline static std::vector<bool> _iknpSenderChoices0;  // For sender=0 direction
    inline static std::vector<bool> _iknpSenderChoices1;  // For sender=1 direction

    // RSA keys for BaseOT, pre-generated at initialization
    inline static std::string _baseOtSelfPub;
    inline static std::string _baseOtSelfPri;
    inline static std::string _baseOtOtherPub;

public:
    static void prepareBmt();

    static void prepareRot();

    static void prepareBaseOtRsaKeys();


    static void startGenerateBmtsAsync();

    static void startGenerateBitwiseBmtsAsync();

    static void prepareIknp();

    static void init();

    static void finalize();

    static std::vector<Bmt> pollBmts(int count, int width);

    static std::vector<BitwiseBmt> pollBitwiseBmts(int count, int width);
};


#endif
