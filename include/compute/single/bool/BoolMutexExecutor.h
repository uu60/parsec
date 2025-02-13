//
// Created by 杜建璋 on 2025/1/8.
//

#ifndef BOOLMUTEXEXECUTOR_H
#define BOOLMUTEXEXECUTOR_H
#include "./BoolExecutor.h"
#include "../../../intermediate/item/BitwiseBmt.h"


class BoolMutexExecutor : public BoolExecutor {
private:
    int64_t _cond_i{};
    std::vector<BitwiseBmt> *_bmts{};
    // Bmt* _bmt{};

public:
    BoolMutexExecutor(int64_t x, int64_t y, bool cond, int width, int16_t taskTag, int16_t msgTagOffset, int clientRank);

    BoolMutexExecutor *execute() override;

    BoolMutexExecutor *setBmts(std::vector<BitwiseBmt> *bmts);

    static int16_t msgTagCount(int l);

    static int bmtCount(int width);
};


#endif //BOOLMUTEXEXECUTOR_H
