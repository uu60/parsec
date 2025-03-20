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

public:
    inline static std::atomic_int64_t _totalTime = 0;
    // inline static std::atomic_int64_t _part0 = 0;
    // inline static std::atomic_int64_t _part1 = 0;
    // inline static std::atomic_int64_t _part2 = 0;

public:
    BoolAndBatchExecutor(std::vector<int64_t> xs, std::vector<int64_t> ys, int l, int taskTag, int msgTagOffset,
                       int clientRank) : BoolBatchExecutor(xs, ys, l, taskTag, msgTagOffset, clientRank) {
    }

    BoolAndBatchExecutor *execute() override;

    [[nodiscard]] static int msgTagCount(int num, int width);

    BoolAndBatchExecutor *setBmts(std::vector<BitwiseBmt> *bmts);

    static int bmtCount(int num);

private:
    int prepareBmts(std::vector<BitwiseBmt> &bmts);
};


#endif //BOOLANDBATCHEXECUTOR_H
