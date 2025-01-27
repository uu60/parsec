//
// Created by 杜建璋 on 2024/11/12.
//

#ifndef INTANDEXECUTOR_H
#define INTANDEXECUTOR_H
#include "./BoolExecutor.h"
#include "../../intermediate/item/Bmt.h"

class BoolAndExecutor : public BoolExecutor {
private:
    std::vector<Bmt> *_bmts{};

public:
    BoolAndExecutor(int64_t x, int64_t y, int l, int16_t taskTag, int16_t msgTagOffset,
                       int clientRank) : BoolExecutor(x, y, l, taskTag, msgTagOffset, clientRank) {
    };

    BoolAndExecutor *execute() override;

    [[nodiscard]] std::string className() const override;

    [[nodiscard]] static int16_t needMsgTags(int l);

    static std::pair<int, int> needBmtsWithBits(int l);

    BoolAndExecutor *setBmts(std::vector<Bmt> *bmt);
};


#endif //INTANDEXECUTOR_H
