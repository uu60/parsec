
#ifndef INTANDEXECUTOR_H
#define INTANDEXECUTOR_H
#include "parsecdb/compute/single/bool/BoolOperator.h"
#include "parsecdb/intermediate/item/BitwiseBmt.h"

namespace parsecdb {

class BoolAndOperator : public BoolOperator {
  private:
    BitwiseBmt* _bmt{};

  public:
    inline static std::atomic_int64_t _totalTime = 0;

  public:
    BoolAndOperator(int64_t x, int64_t y, int l, int taskTag, int msgTagOffset, int clientRank)
        : BoolOperator(x, y, l, taskTag, msgTagOffset, clientRank) {}

    BoolAndOperator* execute() override;

    [[nodiscard]] static int tagStride(int width);

    BoolAndOperator* setBmt(BitwiseBmt* bmt);
};

} // namespace parsecdb

#endif
