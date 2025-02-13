//
// Created by 杜建璋 on 2024/10/23.
//

#ifndef MPC_PACKAGE_MUXEXECUTOR_H
#define MPC_PACKAGE_MUXEXECUTOR_H
#include "./ArithExecutor.h"
#include "../../../intermediate/item/BitwiseBmt.h"

class ArithMutexExecutor : public ArithExecutor {
private:
    int64_t _cond_i{};
    std::vector<Bmt> *_bmts{};
    // BitwiseBmt* _bmt{};

public:
    ArithMutexExecutor(int64_t x, int64_t y, bool c, int l, int16_t taskTag, int16_t msgTagOffset, int clientRank);

    ArithMutexExecutor *execute() override;

    static int16_t msgTagCount(int width);

    ArithMutexExecutor *setBmts(std::vector<Bmt> *bmts);

    static int bmtCount(int width);
};

#endif //MPC_PACKAGE_MUXEXECUTOR_H
