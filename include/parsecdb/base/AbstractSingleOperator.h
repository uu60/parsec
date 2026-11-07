
#ifndef ABSTRACTSINGLEEXECUTOR_H
#define ABSTRACTSINGLEEXECUTOR_H
#include "parsecdb/base/SecureOperator.h"

namespace parsecdb {

class AbstractSingleOperator : public SecureOperator {
  public:
    int64_t _result{};
    int64_t _xi{};
    int64_t _yi{};
    int64_t _zi{};

    AbstractSingleOperator(int width, int taskTag, int msgTagOffset) : SecureOperator(width, taskTag, msgTagOffset) {}
};

} // namespace parsecdb

#endif
