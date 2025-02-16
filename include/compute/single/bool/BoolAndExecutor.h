//
// Created by 杜建璋 on 2024/11/12.
//

#ifndef INTANDEXECUTOR_H
#define INTANDEXECUTOR_H
#include "./BoolExecutor.h"
#include "../../../intermediate/item/BitwiseBmt.h"

class BoolAndExecutor : public BoolExecutor {
private:
    // std::vector<Bmt> *_bmts{};
    BitwiseBmt* _bmt{};

public:
    inline static std::atomic_int64_t _totalTime = 0;

public:
    BoolAndExecutor(int64_t x, int64_t y, int l, int16_t taskTag, int16_t msgTagOffset,
                       int clientRank) : BoolExecutor(x, y, l, taskTag, msgTagOffset, clientRank) {
    }

    BoolAndExecutor *execute() override;

    [[nodiscard]] static int16_t msgTagCount(int width);

    BoolAndExecutor *setBmt(BitwiseBmt *bmt);
};


#endif //INTANDEXECUTOR_H
