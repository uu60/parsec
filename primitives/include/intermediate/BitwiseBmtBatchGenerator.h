//
// Created by 杜建璋 on 2025/2/19.
//

#ifndef BITWISEBMTBATCHGENERATOR_H
#define BITWISEBMTBATCHGENERATOR_H
#include "./AbstractBmtBatchGenerator.h"
#include "../base/AbstractBatchOperator.h"
#include "../intermediate/item/BitwiseBmt.h"


class BitwiseBmtBatchGenerator : public AbstractBmtBatchGenerator<BitwiseBmt> {
public:
    inline static std::atomic_int64_t _totalTime = 0;
    int64_t _totalBits{};
    int _bc{};

    BitwiseBmtBatchGenerator(int count, int width, int taskTag, int msgTagOffset);

    BitwiseBmtBatchGenerator *execute() override;

private:
    void generateRandomAB();

    void computeMix(int sender);

    void computeC();

    [[nodiscard]] int64_t corr(int bmtIdx, int64_t x) const;

public:
    SecureOperator * reconstruct(int clientRank) override;

    static int tagStride();
};



#endif //BITWISEBMTBATCHGENERATOR_H
