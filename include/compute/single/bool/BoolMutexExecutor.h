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

public:
    inline static std::atomic_int64_t _totalTime = 0;

public:
    BoolMutexExecutor(int64_t x, int64_t y, bool cond, int width, int taskTag, int msgTagOffset, int clientRank);

    BoolMutexExecutor *execute() override;

    BoolMutexExecutor *setBmts(std::vector<BitwiseBmt> *bmts);

    static int msgTagCount(int width);

    static int bmtCount();

private:
    bool prepareBmts(std::vector<BitwiseBmt> &bmts);
};


#endif //BOOLMUTEXEXECUTOR_H
