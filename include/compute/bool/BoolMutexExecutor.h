//
// Created by 杜建璋 on 2025/1/8.
//

#ifndef BOOLMUTEXEXECUTOR_H
#define BOOLMUTEXEXECUTOR_H
#include "./BoolExecutor.h"
#include "../../intermediate/item/Bmt.h"


class BoolMutexExecutor : public BoolExecutor {
private:
    int64_t _cond_i{};
    // std::vector<Bmt> *_bmts{};
    Bmt* _bmt{};

public:
    BoolMutexExecutor(int64_t x, int64_t y, bool cond, int l, int16_t taskTag, int16_t msgTagOffset, int clientRank);

    BoolMutexExecutor *execute() override;

    // static int needBmts(int l);

    BoolMutexExecutor *setBmt(Bmt *bmt);

    // static int16_t needMsgTags();
};


#endif //BOOLMUTEXEXECUTOR_H
