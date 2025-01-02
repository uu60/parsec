//
// Created by 杜建璋 on 2024/12/29.
//

#ifndef ABSTRACTOTEXECUTOR_H
#define ABSTRACTOTEXECUTOR_H
#include "../AbstractSecureExecutor.h"


class AbstractOtExecutor : public AbstractSecureExecutor {
protected:
    int64_t _m0{};
    int64_t _m1{};
    int _choice; // msg choice
    // correspond mpi rank
    bool _isSender{};

public:
    AbstractOtExecutor(int sender, int64_t m0, int64_t m1, int choice, int l, int16_t taskTag, int16_t msgTagOffset);

    AbstractOtExecutor *reconstruct(int clientRank) override;
};



#endif //ABSTRACTOTEXECUTOR_H
