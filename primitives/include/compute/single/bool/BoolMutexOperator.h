
#ifndef BOOLMUTEXEXECUTOR_H
#define BOOLMUTEXEXECUTOR_H
#include "./BoolOperator.h"
#include "../../../intermediate/item/BitwiseBmt.h"


class BoolMutexOperator : public BoolOperator {
private:
    int64_t _cond_i{};
    std::vector<BitwiseBmt> *_bmts{};

public:
    inline static std::atomic_int64_t _totalTime = 0;

public:
    BoolMutexOperator(int64_t x, int64_t y, bool cond, int width, int taskTag, int msgTagOffset, int clientRank);

    BoolMutexOperator *execute() override;

    BoolMutexOperator *setBmts(std::vector<BitwiseBmt> *bmts);

    static int tagStride(int width);

    static int bmtCount();

private:
    bool prepareBmts(std::vector<BitwiseBmt> &bmts);
};


#endif
