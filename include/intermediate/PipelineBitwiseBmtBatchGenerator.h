//
// Created by 杜建璋 on 25-4-18.
//

#ifndef PIPELINEBITWISEBMTBATCHGENERATOR_H
#define PIPELINEBITWISEBMTBATCHGENERATOR_H
#include "AbstractBmtBatchGenerator.h"
#include "item/BitwiseBmt.h"
#include "sync/BoostSpscQueue.h"

class PipelineBitwiseBmtBatchGenerator : public AbstractBatchOperator {
private:
    int64_t _totalBits{};
    // Write NO._index queue
    int _index{};
    BoostSPSCQueue<std::vector<BitwiseBmt>, INT16_MAX> _bmts{};
    BoostSPSCQueue<std::vector<int64_t>, INT16_MAX> _ssis{};

    class HandleData {
    public:
        std::vector<int64_t> *_recv{};
        std::vector<int64_t> *_choiceBits{};
        AbstractRequest *_r{};
    };

    BoostSPSCQueue<HandleData, INT16_MAX> _handle{};

public:
    PipelineBitwiseBmtBatchGenerator(int index, int taskTag, int msgTagOffset) : AbstractBatchOperator(64, taskTag,
            msgTagOffset), _index(index) {
    }

    PipelineBitwiseBmtBatchGenerator *execute() override;

private:
    void generateRandomAB(std::vector<int64_t> &as, std::vector<int64_t> &bs);

    void compute(int sender, std::vector<int64_t> &as, std::vector<int64_t> &bs);

    void otAsync(int sender, std::vector<int64_t> &bits0, std::vector<int64_t> &bits1,
                 std::vector<int64_t> &choiceBits);

    void mainThreadHandle();

    void subThreadHandle();

    [[nodiscard]] static int64_t corr(std::vector<int64_t> &as, int bmtIdx, int64_t x);

public:
    PipelineBitwiseBmtBatchGenerator *reconstruct(int clientRank) override;
};


#endif //PIPELINEBITWISEBMTBATCHGENERATOR_H
