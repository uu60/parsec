//
// Created by 杜建璋 on 2025/2/1.
//

#ifndef BITWISEBMTGENERATOR_H
#define BITWISEBMTGENERATOR_H
#include "AbstractBmtSingleGenerator.h"
#include "./item/Bmt.h"
#include "../base/AbstractSingleExecutor.h"
#include "./item/BitwiseBmt.h"


class BitwiseBmtGenerator : public AbstractBmtSingleGenerator<BitwiseBmt> {
public:
    inline static std::atomic_int64_t _totalTime = 0;

    explicit BitwiseBmtGenerator(int l, int16_t taskTag, int16_t msgTagOffset) : AbstractBmtSingleGenerator(
        l, taskTag, msgTagOffset) {
    };


    BitwiseBmtGenerator *execute() override;

private:
    void generateRandomAB();

    void computeMix(int sender);

    void computeC();

    [[nodiscard]] int64_t corr(int i, int64_t x) const;

public:
    AbstractSecureExecutor * reconstruct(int clientRank) override;

    static int16_t msgTagCount(int width);
};


#endif //BITWISEBMTGENERATOR_H
