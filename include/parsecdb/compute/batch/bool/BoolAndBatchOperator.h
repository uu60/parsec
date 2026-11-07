
#ifndef BOOLANDBATCHEXECUTOR_H
#define BOOLANDBATCHEXECUTOR_H
#include "parsecdb/compute/batch/bool/BoolBatchOperator.h"
#include "parsecdb/intermediate/item/BitwiseBmt.h"

namespace parsecdb {

class BoolAndBatchOperator : public BoolBatchOperator {
  private:
    std::vector<BitwiseBmt>* _bmts{};

    std::vector<int64_t>* _conds_i{};
    bool _doWithConditions{};

  public:
    inline static std::atomic_int64_t _totalTime = 0;

  public:
    BoolAndBatchOperator(std::vector<int64_t>* xs, std::vector<int64_t>* ys, int width, int taskTag, int msgTagOffset,
                         int clientRank)
        : BoolBatchOperator(xs, ys, width, taskTag, msgTagOffset, clientRank) {}

    BoolAndBatchOperator(std::vector<int64_t>* xs, std::vector<int64_t>* ys, std::vector<int64_t>* conds, int width,
                         int taskTag, int msgTagOffset);

    BoolAndBatchOperator* execute() override;

    [[nodiscard]] static int tagStride();

    BoolAndBatchOperator* setBmts(std::vector<BitwiseBmt>* bmts);

    static int bmtCount(int num, int width);

  private:
    void execute0();

    void executeForMutex();

    int prepareBmts(std::vector<BitwiseBmt>& bmts);
};

} // namespace parsecdb

#endif
