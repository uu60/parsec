//
// Created by 杜建璋 on 2025/1/31.
//

#ifndef ABSTRACTOTBATCHEXECUTOR_H
#define ABSTRACTOTBATCHEXECUTOR_H
#include "../../base/AbstractBatchExecutor.h"


class AbstractOtBatchExecutor : public AbstractBatchExecutor {
protected:
    std::vector<int64_t> *_ms0{};
    std::vector<int64_t> *_ms1{};
    std::vector<int> *_choices{}; // msg choice
    // correspond mpi rank
    bool _isSender{};

public:
    AbstractOtBatchExecutor(int width, int taskTag, int msgTagOffset) : AbstractBatchExecutor(
        width, taskTag, msgTagOffset) {
    };

    AbstractOtBatchExecutor(int sender, std::vector<int64_t> *ms0, std::vector<int64_t> *ms1, std::vector<int> *choices,
                            int width, int taskTag, int msgTagOffset);

    AbstractOtBatchExecutor *reconstruct(int clientRank) override;
};


#endif //ABSTRACTOTBATCHEXECUTOR_H
