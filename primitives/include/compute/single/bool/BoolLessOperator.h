
#ifndef BOOLLESSTHANEXECUTOR_H
#define BOOLLESSTHANEXECUTOR_H
#include "./BoolOperator.h"
#include "../../../intermediate/item/BitwiseBmt.h"

class BoolLessOperator : public BoolOperator {
private:
    std::vector<BitwiseBmt> *_bmts{};

public:
    inline static std::atomic_int64_t _totalTime = 0;

public:
    BoolLessOperator(int64_t x, int64_t y, int l, int taskTag, int msgTagOffset, int clientRank) : BoolOperator(y, x, l, taskTag, msgTagOffset, clientRank) {};

    BoolLessOperator *execute() override;

    BoolLessOperator *setBmts(std::vector<BitwiseBmt> *bmts);

    static int tagStride(int width);

    static int bmtCount(int width);

private:
    int64_t shiftGreater(int64_t in, int r) const;

    bool prepareBmts(std::vector<BitwiseBmt> &bmts);
};



#endif
