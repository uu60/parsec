//
// Created by 杜建璋 on 2024/10/23.
//

#ifndef MPC_PACKAGE_MUXEXECUTOR_H
#define MPC_PACKAGE_MUXEXECUTOR_H
#include "./ArithOperator.h"
#include "../../../intermediate/item/BitwiseBmt.h"

class ArithMutexOperator : public ArithOperator {
private:
    int64_t _cond_i{};
    std::vector<Bmt> *_bmts{};
    // BitwiseBmt* _bmt{};

public:
    ArithMutexOperator(int64_t x, int64_t y, bool c, int l, int taskTag, int msgTagOffset, int clientRank);

    ArithMutexOperator *execute() override;

    static int tagStride(int width);

    ArithMutexOperator *setBmts(std::vector<Bmt> *bmts);

    static int bmtCount(int width);
};

#endif //MPC_PACKAGE_MUXEXECUTOR_H
