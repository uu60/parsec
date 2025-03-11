//
// Created by 杜建璋 on 2024/9/6.
//

#ifndef MPC_PACKAGE_INTSHAREEXECUTOR_H
#define MPC_PACKAGE_INTSHAREEXECUTOR_H

#include <vector>

#include "../../../base/AbstractSingleExecutor.h"


class ArithExecutor : public AbstractSingleExecutor {
public:
    /**
     * In the constructor, data will be prepared before computing (network maybe needed).
     * Only ArithExecutor and BoolExecutor can set zi (unreconstructed result) directly in the constructor
     * which has an argument of z.
     *
     * @param z Straightly store computed unreconstructed result.
     * @param width Length of numbers in this MPC process.
     * @param taskTag Object tag.
     * @param msgTagOffset Offset of start msg tag
     * @param clientRank If clientRank is negative, means locally set value. Else clientRank represents sharer's rank.
     */
    ArithExecutor(int64_t z, int width, int taskTag, int msgTagOffset, int clientRank);

    ArithExecutor(int64_t x, int64_t y, int width, int taskTag, int msgTagOffset, int clientRank);

    ArithExecutor *reconstruct(int clientRank) override;

    [[deprecated("This function should not be called.")]]
    ArithExecutor *execute() override;
};


#endif //MPC_PACKAGE_INTSHAREEXECUTOR_H
