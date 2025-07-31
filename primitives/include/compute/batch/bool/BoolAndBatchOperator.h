//
// Created by 杜建璋 on 2025/2/24.
//

#ifndef BOOLANDBATCHEXECUTOR_H
#define BOOLANDBATCHEXECUTOR_H
#include "./BoolBatchOperator.h"
#include "../../../intermediate/item/BitwiseBmt.h"

class BoolAndBatchOperator : public BoolBatchOperator {
private:
    // std::vector<Bmt> *_bmts{};
    std::vector<BitwiseBmt>* _bmts{};

    // for mutex
    std::vector<int64_t> *_conds_i{};
    bool _doWithConditions{};

public:
    inline static std::atomic_int64_t _totalTime = 0;

public:
    BoolAndBatchOperator(std::vector<int64_t> *xs, std::vector<int64_t> *ys, int width, int taskTag, int msgTagOffset,
                       int clientRank) : BoolBatchOperator(xs, ys, width, taskTag, msgTagOffset, clientRank) {
    }

    // For mutex. Do [xs, ys] & [conds, conds]
    BoolAndBatchOperator(std::vector<int64_t> *xs, std::vector<int64_t> *ys, std::vector<int64_t> *conds, int width, int taskTag, int msgTagOffset);

    BoolAndBatchOperator *execute() override;

    [[nodiscard]] static int tagStride();

    BoolAndBatchOperator *setBmts(std::vector<BitwiseBmt> *bmts);

    // *2 needed when for mutex
    static int bmtCount(int num, int width);

private:
    void execute0();

    void executeForMutex();

    int prepareBmts(std::vector<BitwiseBmt> &bmts);
};


#endif //BOOLANDBATCHEXECUTOR_H
