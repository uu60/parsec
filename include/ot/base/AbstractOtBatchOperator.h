//
// Created by 杜建璋 on 2025/1/31.
//

#ifndef ABSTRACTOTBATCHEXECUTOR_H
#define ABSTRACTOTBATCHEXECUTOR_H
#include "../../base/AbstractBatchOperator.h"


class AbstractOtBatchOperator : public AbstractBatchOperator {
protected:
    std::vector<int64_t> *_ms0{};
    std::vector<int64_t> *_ms1{};
    std::vector<int> *_choices{}; // msg choice
    // correspond mpi rank
    bool _isSender{};

public:
    AbstractOtBatchOperator(int width, int taskTag, int msgTagOffset) : AbstractBatchOperator(
        width, taskTag, msgTagOffset) {
    };

    AbstractOtBatchOperator(int sender, std::vector<int64_t> *ms0, std::vector<int64_t> *ms1, std::vector<int> *choices,
                            int width, int taskTag, int msgTagOffset);

    AbstractOtBatchOperator *reconstruct(int clientRank) override;
};


#endif //ABSTRACTOTBATCHEXECUTOR_H
