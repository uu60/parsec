
#ifndef BOOLMUTEXEXECUTOR_H
#define BOOLMUTEXEXECUTOR_H
#include "parsecdb/compute/single/bool/BoolOperator.h"
#include "parsecdb/intermediate/item/BitwiseBmt.h"

namespace parsecdb {

class BoolMutexOperator : public BoolOperator {
  private:
    int64_t _cond_i{};
    std::vector<BitwiseBmt>* _bmts{};

  public:
    inline static std::atomic_int64_t _totalTime = 0;

  public:
    BoolMutexOperator(int64_t x, int64_t y, bool cond, int width, int taskTag, int msgTagOffset, int clientRank);

    BoolMutexOperator* execute() override;

    BoolMutexOperator* setBmts(std::vector<BitwiseBmt>* bmts);

    static int tagStride(int width);

    static int bmtCount();

  private:
    bool prepareBmts(std::vector<BitwiseBmt>& bmts);
};

} // namespace parsecdb

#endif
