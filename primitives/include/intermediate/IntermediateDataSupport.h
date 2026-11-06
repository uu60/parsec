#ifndef BMTHOLDER_H
#define BMTHOLDER_H

#include <array>
#include <cstdint>
#include <vector>

#include "./item/BitwiseBmt.h"
#include "./item/Bmt.h"
#include "./item/RRot.h"
#include "./item/SRot.h"
#include "conf/Conf.h"
#include "sync/AbstractBlockingQueue.h"
#include "utils/Crypto.h"

namespace parsec {

class IntermediateDataSupport {
  private:
    inline static u_int _currentBmtQ = 0;
    inline static u_int _currentBitwiseBmtQ = 0;

    inline static Bmt* _currentBmt{};
    inline static BitwiseBmt* _currentBitwiseBmt{};
    inline static int _currentBmtLeftTimes = Conf::BMT_USAGE_LIMIT;
    inline static int _currentBitwiseBmtLeftTimes = Conf::BMT_USAGE_LIMIT;

  public:
    inline static std::vector<AbstractBlockingQueue<Bmt>*> _bmtQs;
    inline static std::vector<AbstractBlockingQueue<BitwiseBmt>*> _bitwiseBmtQs;

    inline static Bmt _fixedBmt;
    inline static BitwiseBmt _fixedBitwiseBmt;

    inline static SRot* _sRot0 = nullptr;
    inline static RRot* _rRot0 = nullptr;
    inline static SRot* _sRot1 = nullptr;
    inline static RRot* _rRot1 = nullptr;

    // IKNP base seeds (128-bit) from 128 base OTs, computed once in init(). Direction 0 has rank 0 as the IKNP
    // sender, direction 1 has rank 1. The IKNP receiver of a direction holds both seeds {k_i^0, k_i^1}; the IKNP
    // sender holds only k_i^{s_i} (stored in element 0) and its secret choice bits s, which are never sent.
    inline static std::vector<std::array<U128, 2>> _iknpBaseSeeds0;
    inline static std::vector<std::array<U128, 2>> _iknpBaseSeeds1;

    // The IKNP sender's secret choice bits s for each direction (only populated on the sender's rank).
    inline static std::vector<bool> _iknpSenderChoices0;
    inline static std::vector<bool> _iknpSenderChoices1;

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

    static void prepareIknpDirection(int iknpSender, int msgTagOffset, std::vector<std::array<U128, 2>>& seeds,
                                     std::vector<bool>& senderChoices);

    static void init();

    static void finalize();

    static std::vector<Bmt> pollBmts(int count, int width);

    static std::vector<BitwiseBmt> pollBitwiseBmts(int count, int width);
};

} // namespace parsec

#endif
