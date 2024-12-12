//
// Created by 杜建璋 on 2024/9/6.
//

#ifndef MPC_PACKAGE_INTSHAREEXECUTOR_H
#define MPC_PACKAGE_INTSHAREEXECUTOR_H

#include "../AbstractSecureExecutor.h"
#include <vector>

class ArithExecutor : public AbstractSecureExecutor {
private:
    static int32_t _currentObjTag;
public:
    int64_t _xi{};
    int64_t _yi{};

    /**
     * In the constructor, data will be prepared before computing (network maybe needed).
     * Only ArithExecutor and BoolExecutor can set zi (unreconstructed result) directly in the constructor
     * which has an argument of z.
     *
     * @param z Straightly store computed unreconstructed result.
     * @param l Length of numbers in this MPC process.
     * @param objTag Object tag.
     * @param clientRank If clientRank is negative, means locally set value. Else clientRank represents sharer's rank.
     */
    ArithExecutor(int64_t z, int l, int16_t objTag, int16_t msgTagOffset, int clientRank);

    ArithExecutor(int64_t x, int64_t y, int l, int16_t objTag, int16_t msgTagOffset, int clientRank);

    ArithExecutor *reconstruct(int clientRank) override;

    [[deprecated("This function should not be called.")]]
    ArithExecutor *execute() override;

    [[deprecated("This function should not be called.")]]
    [[nodiscard]] std::string className() const override;
};


#endif //MPC_PACKAGE_INTSHAREEXECUTOR_H
