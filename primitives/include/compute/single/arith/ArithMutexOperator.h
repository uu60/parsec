
#ifndef MPC_PACKAGE_MUXEXECUTOR_H
#define MPC_PACKAGE_MUXEXECUTOR_H
#include "../../../intermediate/item/BitwiseBmt.h"
#include "./ArithOperator.h"

namespace parsec {

class ArithMutexOperator : public ArithOperator {
  private:
    int64_t _cond_i{};
    std::vector<Bmt>* _bmts{};

  public:
    ArithMutexOperator(int64_t x, int64_t y, bool c, int l, int taskTag, int msgTagOffset, int clientRank);

    ArithMutexOperator* execute() override;

    static int tagStride(int width);

    ArithMutexOperator* setBmts(std::vector<Bmt>* bmts);

    static int bmtCount(int width);
};

} // namespace parsec

#endif
