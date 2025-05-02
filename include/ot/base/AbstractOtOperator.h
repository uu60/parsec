//
// Created by 杜建璋 on 2024/12/29.
//

#ifndef ABSTRACTOTEXECUTOR_H
#define ABSTRACTOTEXECUTOR_H
#include "../../base/AbstractSingleOperator.h"


class AbstractOtOperator : public AbstractSingleOperator {
protected:
    int64_t _m0{};
    int64_t _m1{};
    int _choice; // msg choice
    // correspond mpi rank
    bool _isSender{};

public:
    AbstractOtOperator(int sender, int64_t m0, int64_t m1, int choice, int l, int taskTag, int msgTagOffset);

    AbstractOtOperator *reconstruct(int clientRank) override;
};



#endif //ABSTRACTOTEXECUTOR_H
