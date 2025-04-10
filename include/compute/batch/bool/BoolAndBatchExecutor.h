//
// Created by 杜建璋 on 2025/2/24.
//

#ifndef BOOLANDBATCHEXECUTOR_H
#define BOOLANDBATCHEXECUTOR_H
#include "./BoolBatchExecutor.h"
#include "../../../intermediate/item/BitwiseBmt.h"

class BoolAndBatchExecutor : public BoolBatchExecutor {
private:
    // std::vector<Bmt> *_bmts{};
    std::vector<BitwiseBmt>* _bmts{};

    // for mutex
    std::vector<int64_t> *_conds_i{};
    bool _doMutex{};

public:
    inline static std::atomic_int64_t _totalTime = 0;

public:
    BoolAndBatchExecutor(std::vector<int64_t> *xs, std::vector<int64_t> *ys, int width, int taskTag, int msgTagOffset,
                       int clientRank) : BoolBatchExecutor(xs, ys, width, taskTag, msgTagOffset, clientRank) {
    }

    // For mutex. Do [xs, ys] & [conds, conds]
    BoolAndBatchExecutor(std::vector<int64_t> *xs, std::vector<int64_t> *ys, std::vector<int64_t> *conds, int width, int taskTag, int msgTagOffset);

    BoolAndBatchExecutor *execute() override;

    [[nodiscard]] static int msgTagCount();

    BoolAndBatchExecutor *setBmts(std::vector<BitwiseBmt> *bmts);

    static int bmtCount(int num);

private:
    void execute0();

    void executeForMutex();

    int prepareBmts(std::vector<BitwiseBmt> &bmts);
};


#endif //BOOLANDBATCHEXECUTOR_H
